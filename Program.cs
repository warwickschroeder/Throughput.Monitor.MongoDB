using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using MongoDB.Bson;
using MongoDB.Driver;

// Configuration - uses environment variables with sensible defaults for local dev
var rabbitManagementUrl = Environment.GetEnvironmentVariable("RABBIT_MANAGEMENT_URL") ?? "http://localhost:15672";
var rabbitUser = Environment.GetEnvironmentVariable("RABBIT_USER") ?? "guest";
var rabbitPass = Environment.GetEnvironmentVariable("RABBIT_PASS") ?? "guest";
var rabbitVhost = Environment.GetEnvironmentVariable("RABBIT_VHOST") ?? "%2F"; // URL-encoded "/"
var queueName = Environment.GetEnvironmentVariable("RABBIT_QUEUE") ?? "audit";

var mongoConnectionString = Environment.GetEnvironmentVariable("MONGO_CONNECTION_STRING") ?? "mongodb://localhost:27017";
var mongoDatabaseName = Environment.GetEnvironmentVariable("MONGO_DATABASE") ?? "audit";
var collectionName = Environment.GetEnvironmentVariable("MONGO_COLLECTION") ?? "processedMessages";

var pollIntervalMs = 2000;
var maxRunTime = TimeSpan.FromMinutes(60);

// Thresholds (matching CheckMongoDbCachePressure)
const double dirtyThreshold = 15.0;
const double latencyThreshold = 500.0;

// Setup RabbitMQ HTTP client
using var http = new HttpClient { BaseAddress = new Uri(rabbitManagementUrl) };
http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
    "Basic", Convert.ToBase64String(Encoding.ASCII.GetBytes($"{rabbitUser}:{rabbitPass}")));

// Setup MongoDB client
var mongoClient = new MongoClient(mongoConnectionString);
var db = mongoClient.GetDatabase(mongoDatabaseName);
var collection = db.GetCollection<BsonDocument>(collectionName);
var bodiesCollection = db.GetCollection<BsonDocument>("messageBodies");

// State for delta calculations
long? prevMongoCount = null;
long? prevBodyCount = null;
long? prevTtlDeleted = null;
long prevTtlPasses = 0;
DateTime prevTime = DateTime.UtcNow;
int rowCount = 0;
const int headerRepeatInterval = 20;

// Queue depth trend tracking
const int trendWindowSize = 15; // 30 seconds of samples at 2s intervals
var queueDepthWindow = new Queue<long>();
long? prevQueueDepth = null;
int consecutiveGrowth = 0;

// Body lag smoothing (60s window = 30 samples at 2s intervals, matches TTL monitor cycle)
const int bodyLagSmoothingWindow = 30;
var bodyLagWindow = new Queue<long>();

// Summary stats accumulators
var summaryStats = new SummaryStats();

// CSV log file
var logPath = Path.Combine(AppContext.BaseDirectory, $"throughput-{DateTime.Now:yyyyMMdd-HHmmss}.csv");
await using var logWriter = new StreamWriter(logPath, append: false);
await logWriter.WriteLineAsync("Timestamp,RMQ_Publish_MsgSec,RMQ_Deliver_MsgSec,RMQ_Ack_MsgSec,Queue_Depth,Mongo_Docs,Mongo_MsgSec,Body_MsgSec,Body_Lag,TTL_Del_Sec,TTL_Pass,Dirty_Pct,Used_Pct,Ping_Ms,Data_MB,Storage_MB,Index_MB,Queue_Trend,Status");

Console.WriteLine($"Throughput Monitor - logging to {logPath}");
Console.WriteLine($"RabbitMQ: {rabbitManagementUrl} queue={queueName}  |  MongoDB: {mongoDatabaseName}.{collectionName}");
Console.WriteLine($"Thresholds: dirty >= {dirtyThreshold}%, latency >= {latencyThreshold}ms  |  Max run: {maxRunTime.TotalMinutes}min");
Console.WriteLine();
PrintHeaders();

var startTime = DateTime.UtcNow;
using var cts = new CancellationTokenSource(maxRunTime);

// Ensure Ctrl+C triggers graceful shutdown (summary will print)
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // Prevent immediate termination
    cts.Cancel();
};

try
{

while (!cts.Token.IsCancellationRequested)
{
    try
    {
        var now = DateTime.UtcNow;
        var elapsed = (now - prevTime).TotalSeconds;

        // --- RabbitMQ (use built-in smoothed rates from Management API) ---
        long queueDepth = 0;
        double publishRate = 0;
        double deliverRate = 0;
        double ackRate = 0;

        try
        {
            var response = await http.GetAsync($"/api/queues/{rabbitVhost}/{queueName}");
            response.EnsureSuccessStatusCode();
            var json = await response.Content.ReadAsStringAsync();

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            queueDepth = root.GetProperty("messages").GetInt64();

            if (root.TryGetProperty("message_stats", out var stats))
            {
                publishRate = GetRate(stats, "publish_details");
                deliverRate = GetRate(stats, "deliver_get_details");
                ackRate = GetRate(stats, "ack_details");
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"  RabbitMQ error: {ex.Message}");
        }

        // --- MongoDB doc count ---
        long mongoCount = 0;
        try
        {
            mongoCount = await collection.EstimatedDocumentCountAsync();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"  MongoDB error: {ex.Message}");
        }

        // --- MongoDB body count ---
        long bodyCount = 0;
        try
        {
            bodyCount = await bodiesCollection.EstimatedDocumentCountAsync();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"  MongoDB bodies error: {ex.Message}");
        }

        // --- MongoDB health: WiredTiger cache + ping latency + storage ---
        double dirtyPct = 0;
        double usedPct = 0;
        double pingMs = 0;
        long ttlDeletedTotal = 0;
        long ttlPasses = 0;
        double dataMb = 0;
        double storageMb = 0;
        double indexMb = 0;
        var status = "OK";

        try
        {
            // Ping latency
            var sw = Stopwatch.StartNew();
            await db.RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1));
            sw.Stop();
            pingMs = sw.Elapsed.TotalMilliseconds;

            // serverStatus for WiredTiger + TTL metrics
            var serverStatus = await db.RunCommandAsync<BsonDocument>(new BsonDocument("serverStatus", 1));

            // WiredTiger cache metrics
            if (serverStatus.Contains("wiredTiger"))
            {
                var cache = serverStatus["wiredTiger"].AsBsonDocument["cache"].AsBsonDocument;
                var dirtyBytes = cache["tracked dirty bytes in the cache"].ToDouble();
                var totalBytes = cache["bytes currently in the cache"].ToDouble();
                var maxBytes = cache["maximum bytes configured"].ToDouble();

                dirtyPct = maxBytes > 0 ? dirtyBytes / maxBytes * 100 : 0;
                usedPct = maxBytes > 0 ? totalBytes / maxBytes * 100 : 0;

                if (dirtyPct >= dirtyThreshold)
                {
                    status = "DIRTY!";
                }
            }

            // TTL metrics
            if (serverStatus.Contains("metrics"))
            {
                var ttl = serverStatus["metrics"].AsBsonDocument["ttl"].AsBsonDocument;
                ttlDeletedTotal = ttl["deletedDocuments"].ToInt64();
                ttlPasses = ttl["passes"].ToInt64();
            }

            if (pingMs >= latencyThreshold)
            {
                status = "SLOW!";
            }

            // Database storage size
            var dbStats = await db.RunCommandAsync<BsonDocument>(new BsonDocument("dbStats", 1));
            dataMb = dbStats["dataSize"].ToDouble() / (1024 * 1024);
            storageMb = dbStats["storageSize"].ToDouble() / (1024 * 1024);
            indexMb = dbStats["indexSize"].ToDouble() / (1024 * 1024);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"  MongoDB health error: {ex.Message}");
            status = "ERR";
        }

        // --- Output ---
        if (prevMongoCount.HasValue && elapsed > 0)
        {
            var ttlDeletedDelta = prevTtlDeleted.HasValue ? (ttlDeletedTotal - prevTtlDeleted.Value) : 0;
            var ttlDelPerSec = ttlDeletedDelta / elapsed;

            // Split TTL deletes proportionally between collections (TTL metric is server-wide)
            double mongoTtlShare, bodyTtlShare;
            if (bodyCount > 0 && mongoCount > 0)
            {
                var totalDocs = (double)(mongoCount + bodyCount);
                mongoTtlShare = ttlDeletedDelta * (mongoCount / totalDocs);
                bodyTtlShare = ttlDeletedDelta * (bodyCount / totalDocs);
            }
            else
            {
                mongoTtlShare = ttlDeletedDelta;
                bodyTtlShare = 0;
            }

            var mongoRate = (mongoCount - prevMongoCount.Value + mongoTtlShare) / elapsed;
            var bodyRate = prevBodyCount.HasValue ? (bodyCount - prevBodyCount.Value + bodyTtlShare) / elapsed : 0;

            // Smooth body lag over 60s window to dampen TTL timing skew between collections
            var rawBodyLag = bodyCount == 0 && bodyRate == 0 ? 0 : mongoCount - bodyCount;
            bodyLagWindow.Enqueue(rawBodyLag);
            while (bodyLagWindow.Count > bodyLagSmoothingWindow) bodyLagWindow.Dequeue();
            var bodyLag = (long)bodyLagWindow.Average();
            var ttlPassDelta = ttlPasses - prevTtlPasses;
            var ttlFlag = ttlPassDelta > 0 ? "TTL" : "";
            var timestamp = DateTime.Now.ToString("HH:mm:ss.f");

            // Queue depth trend detection
            if (prevQueueDepth.HasValue)
            {
                if (queueDepth > prevQueueDepth.Value)
                    consecutiveGrowth++;
                else if (queueDepth < prevQueueDepth.Value)
                    consecutiveGrowth = Math.Min(consecutiveGrowth - 1, 0); // decay towards negative
                else
                    consecutiveGrowth = 0; // stable resets
            }
            prevQueueDepth = queueDepth;

            // Sliding window for trend slope
            queueDepthWindow.Enqueue(queueDepth);
            while (queueDepthWindow.Count > trendWindowSize) queueDepthWindow.Dequeue();

            // Calculate net queue growth rate (msgs/sec) over the window
            double queueGrowthRate = 0;
            if (queueDepthWindow.Count >= 2)
            {
                var windowItems = queueDepthWindow.ToArray();
                queueGrowthRate = (windowItems[^1] - windowItems[0]) / ((queueDepthWindow.Count - 1) * (pollIntervalMs / 1000.0));
            }

            // Trend indicator
            var trend = consecutiveGrowth >= 5 ? "BACKLOG"
                      : consecutiveGrowth >= 2 ? "growing"
                      : consecutiveGrowth <= -2 ? "drain"
                      : "stable";

            // Override status for backlog
            if (consecutiveGrowth >= 5 && status == "OK")
            {
                status = "BACKLOG";
            }

            // Track summary stats
            summaryStats.Record(publishRate, deliverRate, ackRate, queueDepth, mongoRate,
                bodyRate, bodyLag, ttlDelPerSec, dirtyPct, usedPct, pingMs, dataMb, storageMb, indexMb, queueGrowthRate, status);

            if (rowCount > 0 && rowCount % headerRepeatInterval == 0)
            {
                PrintHeaders();
            }
            rowCount++;

            Console.WriteLine(
                $"{timestamp,-12} | {publishRate,8:F1} | {deliverRate,8:F1} | {ackRate,8:F1} | {queueDepth,8:N0} | {mongoCount,10:N0} | {mongoRate,8:F1} | {bodyRate,8:F1} | {bodyLag,8:N0} | {ttlDelPerSec,8:F0} | {ttlFlag,4} | {dirtyPct,6:F1}% | {usedPct,6:F1}% | {pingMs,6:F0}ms | {storageMb,6:F0}MB | {trend,7} | {status}");

            await logWriter.WriteLineAsync(
                $"{DateTime.Now:O},{publishRate:F1},{deliverRate:F1},{ackRate:F1},{queueDepth},{mongoCount},{mongoRate:F1},{bodyRate:F1},{bodyLag},{ttlDelPerSec:F0},{ttlPasses},{dirtyPct:F1},{usedPct:F1},{pingMs:F0},{dataMb:F1},{storageMb:F1},{indexMb:F1},{queueGrowthRate:F1},{status}");
            await logWriter.FlushAsync();
        }
        else
        {
            Console.WriteLine($"{"Warming up...",-12} | {"---",8} | {"---",8} | {"---",8} | {queueDepth,8:N0} | {mongoCount,10:N0} | {"---",8} | {"---",8} | {"---",8} | {"---",8} | {"",4} | {dirtyPct,6:F1}% | {usedPct,6:F1}% | {pingMs,6:F0}ms | {storageMb,6:F0}MB | {status}");
        }

        prevMongoCount = mongoCount;
        prevBodyCount = bodyCount;
        prevTtlDeleted = ttlDeletedTotal;
        prevTtlPasses = ttlPasses;
        prevTime = now;

        await Task.Delay(pollIntervalMs, cts.Token);
    }
    catch (OperationCanceledException)
    {
        break;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Error: {ex.Message}");
        await Task.Delay(pollIntervalMs);
    }
}
}
finally
{

// --- Summary ---
var totalElapsed = DateTime.UtcNow - startTime;
Console.WriteLine();
Console.WriteLine(new string('=', 80));
Console.WriteLine($"  SUMMARY  |  Duration: {totalElapsed:m\\:ss}  |  Samples: {summaryStats.Count}  |  Log: {logPath}");
Console.WriteLine(new string('=', 80));

if (summaryStats.Count > 0)
{
    Console.WriteLine();
    Console.WriteLine($"  {"Metric",-24} | {"Avg",10} | {"Min",10} | {"Max",10} | {"P95",10}");
    Console.WriteLine($"  {new string('-', 22)} | {new string('-', 10)} | {new string('-', 10)} | {new string('-', 10)} | {new string('-', 10)}");
    PrintStat("RMQ Publish (msg/s)", summaryStats.Publish);
    PrintStat("RMQ Deliver (msg/s)", summaryStats.Deliver);
    PrintStat("RMQ Ack (msg/s)", summaryStats.Ack);
    PrintStat("Queue Depth", summaryStats.QueueDepth);
    PrintStat("Mongo Stored (msg/s)", summaryStats.MongoRate);
    PrintStat("Body Written (msg/s)", summaryStats.BodyRate);
    PrintStat("Body Lag (docs)", summaryStats.BodyLag);
    PrintStat("TTL Deletes (del/s)", summaryStats.TtlDel);
    PrintStat("Dirty Cache (%)", summaryStats.Dirty);
    PrintStat("Used Cache (%)", summaryStats.Used);
    PrintStat("Ping Latency (ms)", summaryStats.Ping);
    PrintStat("Queue Growth (msg/s)", summaryStats.QueueGrowthRate);

    Console.WriteLine();
    Console.WriteLine($"  Storage: Data={summaryStats.DataMb.Max:F1}MB  Storage={summaryStats.StorageMb.Max:F1}MB  Indexes={summaryStats.IndexMb.Max:F1}MB");
    Console.WriteLine($"  Threshold breaches: DIRTY={summaryStats.DirtyCount}  SLOW={summaryStats.SlowCount}  ERR={summaryStats.ErrorCount}  BACKLOG={summaryStats.BacklogCount}");

    // Verdict
    if (summaryStats.BacklogCount > 0)
    {
        var backlogPct = (double)summaryStats.BacklogCount / summaryStats.Count * 100;
        Console.WriteLine();
        Console.WriteLine($"  ** ServiceControl could NOT keep up for {backlogPct:F0}% of the test ({summaryStats.BacklogCount}/{summaryStats.Count} samples)");
        Console.WriteLine($"  ** Peak queue growth: {summaryStats.QueueGrowthRate.Max:F1} msg/s faster than consumption");
    }
    else if (summaryStats.QueueDepth.Max > 100 && summaryStats.QueueDepth.Avg > 50)
    {
        Console.WriteLine();
        Console.WriteLine($"  ** ServiceControl kept up but queue depth was elevated (avg={summaryStats.QueueDepth.Avg:F0}, max={summaryStats.QueueDepth.Max:F0})");
    }
    else
    {
        Console.WriteLine();
        Console.WriteLine($"  ** ServiceControl kept up with the load");
    }
}

Console.WriteLine();

} // finally

static void PrintStat(string name, MetricAccumulator m)
{
    Console.WriteLine($"  {name,-24} | {m.Avg,10:F1} | {m.Min,10:F1} | {m.Max,10:F1} | {m.P95,10:F1}");
}

static void PrintHeaders()
{
    Console.WriteLine($"{"Time",-12} | {"Publish",8} | {"Deliver",8} | {"Ack",8} | {"Queue",8} | {"Mongo",10} | {"Stored",8} | {"Body",8} | {"BLag",8} | {"TTL Del",8} | {"TTL",4} | {"Dirty",8} | {"Used",8} | {"Ping",8} | {"Disk",8} | {"Trend",7} |");
    Console.WriteLine($"{"",12} | {"msg/s",8} | {"msg/s",8} | {"msg/s",8} | {"depth",8} | {"docs",10} | {"msg/s",8} | {"msg/s",8} | {"docs",8} | {"del/s",8} | {"pass",4} | {"cache",8} | {"cache",8} | {"",8} | {"",8} | {"",7} |");
    Console.WriteLine(new string('-', 180));
}

static double GetRate(JsonElement stats, string detailsProperty)
{
    if (stats.TryGetProperty(detailsProperty, out var details) &&
        details.TryGetProperty("rate", out var rate))
    {
        return rate.GetDouble();
    }
    return 0;
}

class MetricAccumulator
{
    readonly List<double> values = [];
    public double Min { get; private set; } = double.MaxValue;
    public double Max { get; private set; } = double.MinValue;
    double sum;

    public void Add(double value)
    {
        values.Add(value);
        sum += value;
        if (value < Min) Min = value;
        if (value > Max) Max = value;
    }

    public double Avg => values.Count > 0 ? sum / values.Count : 0;

    public double P95
    {
        get
        {
            if (values.Count == 0) return 0;
            var sorted = values.OrderBy(v => v).ToList();
            var index = (int)Math.Ceiling(sorted.Count * 0.95) - 1;
            return sorted[Math.Max(0, index)];
        }
    }
}

class SummaryStats
{
    public int Count { get; private set; }
    public MetricAccumulator Publish { get; } = new();
    public MetricAccumulator Deliver { get; } = new();
    public MetricAccumulator Ack { get; } = new();
    public MetricAccumulator QueueDepth { get; } = new();
    public MetricAccumulator MongoRate { get; } = new();
    public MetricAccumulator BodyRate { get; } = new();
    public MetricAccumulator BodyLag { get; } = new();
    public MetricAccumulator TtlDel { get; } = new();
    public MetricAccumulator Dirty { get; } = new();
    public MetricAccumulator Used { get; } = new();
    public MetricAccumulator Ping { get; } = new();
    public MetricAccumulator DataMb { get; } = new();
    public MetricAccumulator StorageMb { get; } = new();
    public MetricAccumulator IndexMb { get; } = new();
    public MetricAccumulator QueueGrowthRate { get; } = new();
    public int DirtyCount { get; private set; }
    public int SlowCount { get; private set; }
    public int ErrorCount { get; private set; }
    public int BacklogCount { get; private set; }

    public void Record(double publish, double deliver, double ack, long queueDepth,
        double mongoRate, double bodyRate, long bodyLag, double ttlDel, double dirty, double used, double ping,
        double dataMb, double storageMb, double indexMb, double queueGrowthRate, string status)
    {
        Count++;
        Publish.Add(publish);
        Deliver.Add(deliver);
        Ack.Add(ack);
        QueueDepth.Add(queueDepth);
        MongoRate.Add(mongoRate);
        BodyRate.Add(bodyRate);
        BodyLag.Add(bodyLag);
        TtlDel.Add(ttlDel);
        Dirty.Add(dirty);
        Used.Add(used);
        Ping.Add(ping);
        DataMb.Add(dataMb);
        StorageMb.Add(storageMb);
        IndexMb.Add(indexMb);
        QueueGrowthRate.Add(queueGrowthRate);

        if (status == "DIRTY!") DirtyCount++;
        if (status == "SLOW!") SlowCount++;
        if (status == "ERR") ErrorCount++;
        if (status == "BACKLOG") BacklogCount++;
    }
}
