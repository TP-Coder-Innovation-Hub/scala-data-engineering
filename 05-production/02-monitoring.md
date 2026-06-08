# Monitoring `` ``

Data pipelines fail silently if you do not monitor them. Monitoring for Scala data systems covers three areas: application logging, JVM metrics, and framework-specific metrics (Spark, Akka).

## Logging

Use SLF4J with Logback. Never use `println` in production:

```scala
import org.slf4j.LoggerFactory

class EventProcessor:
  private val logger = LoggerFactory.getLogger(getClass)

  def process(event: RawEvent): Either[String, CleanEvent] =
    logger.debug(s"Processing event: ${event.id}")
    val result = parse(event)
    result match
      case Right(clean) =>
        logger.info(s"Successfully processed: ${clean.id}")
        Right(clean)
      case Left(error) =>
        logger.warn(s"Failed to process ${event.id}: $error")
        Left(error)
```

Log levels:

| Level | When to Use | Example |
|-------|-------------|---------|
| ERROR | Unrecoverable failure requiring human action | Database connection lost |
| WARN | Recoverable issue worth investigating | High error rate, slow query |
| INFO | Business-significant events | Job started, job completed, records processed |
| DEBUG | Detailed diagnostic information | Individual event processing, query plans |

Set INFO as the default level. Use DEBUG for troubleshooting specific issues. Never log at DEBUG in production permanently -- it generates too much output and slows down the pipeline.

## JVM Metrics

Monitor these JVM metrics for any Scala application:

```scala
// Using the Prometheus JVM client
import io.prometheus.client.hotspot.DefaultExports
import io.prometheus.client.Counter
import io.prometheus.client.Histogram

// Enable standard JVM metrics (memory, GC, threads)
DefaultExports.initialize()

// Custom metrics for your pipeline
val eventsProcessed = Counter.build()
  .name("events_processed_total")
  .help("Total events processed")
  .labelNames("event_type", "status")
  .register()

val processingTime = Histogram.build()
  .name("event_processing_seconds")
  .help("Time to process a single event")
  .labelNames("event_type")
  .register()
```

Key JVM metrics to monitor:

| Metric | Why It Matters | Alert Threshold |
|--------|---------------|-----------------|
| Heap usage | Memory pressure, potential OOM | > 85% sustained |
| GC pause time | Stop-the-world pauses affect latency | > 500ms |
| GC frequency | High frequency = memory pressure | > 10 per minute |
| Thread count | Thread leaks | Unexpected growth |
| CPU usage | Saturation | > 90% sustained |

## Spark Metrics

Spark exposes metrics through its metrics system. Configure in `spark-defaults.conf`:

```properties
spark.metrics.conf.*.sink.prometheus.class=org.apache.spark.metrics.sink.PrometheusServlet
spark.metrics.conf.*.sink.prometheus.path=/metrics
spark.metrics.conf.*.source.jvm.class=org.apache.spark.metrics.source.JvmSource
```

Key Spark metrics:

| Metric | What It Tells You |
|--------|-------------------|
| `executor.MemoryStore.bytesUsed` | How much memory is cached |
| `driver.DAGScheduler.job.activeJobs` | Number of running jobs |
| `executor.threadPool.activeTasks` | Executor utilization |
| `shuffle.bytesRead` / `shuffle.bytesWritten` | Shuffle volume (high = expensive) |
| `application.ExecutorMetrics.JVMHeapUsage` | Per-executor memory |

## Akka Metrics

```scala
import org.apache.pekko.actor.ActorSystem

val system = ActorSystem("pipeline")

// Enable Kamon or Prometheus for Akka metrics
// Key metrics:
// - actor.mailbox.size (queue depth)
// - actor.processing-time (latency per message)
// - actor.errors (failure rate)
// - stream.throughput (elements per second)
// - stream.backpressure (buffer utilization)
```

Monitor these Akka-specific metrics:

| Metric | Why |
|--------|-----|
| Mailbox size | Growing mailbox = slow consumer or missing backpressure |
| Processing time per message | Detect performance degradation |
| Actor restart count | Supervision is firing = recurring failures |
| Stream throughput | Drops indicate backpressure issues |

## Alerting Strategy ``

Set up alerts for:

1. **Pipeline latency**: job completion time exceeding SLA
2. **Error rate**: more than X% of records failing validation
3. **Data freshness**: data not arriving within expected windows
4. **Resource saturation**: memory, CPU, disk approaching limits
5. **Actor supervision**: any actor restarting more than 3 times in 5 minutes

Alert on symptoms (latency, errors), not causes (CPU, memory). A high CPU alert does not tell you what is wrong. A job-exceeded-SLA alert tells you exactly what the user experiences.
