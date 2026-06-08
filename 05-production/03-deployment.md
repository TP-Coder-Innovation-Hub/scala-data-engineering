# Deployment ``

Deploying Scala data systems involves configuring Spark clusters, Akka clusters, and JVM tuning. This guide covers the practical decisions.

## Spark Cluster Modes

### Cluster Manager Options

| Manager | When to Use |
|---------|-------------|
| YARN | Existing Hadoop ecosystem. Most enterprises. |
| Kubernetes | Cloud-native deployments. Elastic scaling. |
| Standalone | Simple setups, testing, small teams. |

### Submitting a Spark Job

```bash
# Build a JAR with sbt
sbt package
# target/scala-3.6.2/your-pipeline_3-0.1.0.jar

# Submit to a cluster
spark-submit \
  --class com.example.EventPipeline \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 20 \
  --executor-cores 4 \
  --executor-memory 8g \
  --driver-memory 4g \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.shuffle.partitions=200 \
  target/scala-3.6.2/your-pipeline_3-0.1.0.jar \
  --input s3://data-lake/raw/events/ \
  --output s3://data-lake/curated/events/
```

### Executor Sizing

Rule of thumb for executor configuration:

- **Cores per executor**: 4-5. More cores = more concurrent tasks per executor. Beyond 5, HDFS throughput becomes a bottleneck.
- **Memory per executor**: Start with 8g. The JVM uses ~75% for execution and storage, the rest for overhead.
- **Number of executors**: Set based on data size. Each executor should process at least 128MB of data per task.

```bash
# For a 500 GB dataset
--num-executors 25 \
--executor-cores 4 \
--executor-memory 8g
# Total: 25 * 4 = 100 concurrent tasks
# 500 GB / 100 = 5 GB per task (healthy)
```

## Akka Cluster

Akka Cluster provides distributed actor systems with automatic member management:

```scala
// application.conf
akka {
  actor.provider = "cluster"

  remote.artery {
    canonical {
      hostname = "127.0.0.1"
      port = 25520
    }
  }

  cluster {
    seed-nodes = [
      "akka://DataPipeline@seed-1:25520",
      "akka://DataPipeline@seed-2:25520"
    ]
    downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
  }
}
```

Seed nodes are the initial contact points for cluster formation. New nodes contact the seed nodes to join. The Split Brain Resolver prevents split-brain scenarios where network partitions cause two independent clusters.

### Deploying Akka Cluster

1. Build a Docker image with your application:
```dockerfile
FROM eclipse-temurin:21-jre
COPY target/scala-3.6.2/pipeline.jar /app/pipeline.jar
ENTRYPOINT ["java", "-jar", "/app/pipeline.jar"]
```

2. Deploy to Kubernetes with a StatefulSet (for stable network identity):
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: pipeline
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: pipeline
        image: your-registry/pipeline:latest
        ports:
        - containerPort: 25520
```

## JVM Tuning for Data Workloads

Data workloads allocate many short-lived objects (parsed records, intermediate results). The default JVM settings are not optimal for this pattern.

### Garbage Collector

| GC | When to Use |
|----|------------|
| G1GC | Default. Good for most Spark jobs. Balanced latency and throughput. |
| ZGC | Low-latency streaming (< 10ms pauses). Use when GC pauses exceed your latency budget. |
| SerialGC | Single-threaded. Only for small test jobs. |

### JVM Flags

```bash
# For Spark executors
-Xms8g -Xmx8g              # Fixed heap size (avoid resizing)
-XX:+UseG1GC                # G1 garbage collector
-XX:MaxGCPauseMillis=200    # Target max GC pause
-XX:+PrintGCDetails         # Log GC details
-XX:+PrintGCDateStamps      # Timestamps in GC logs

# For Akka / low-latency services
-Xms4g -Xmx4g
-XX:+UseZGC                 # ZGC for low latency
-XX:+ZGenerational          # Generational ZGC (JDK 21+)
```

### Memory Settings for Spark

```bash
# Spark memory layout
# --executor-memory 8g
# Breakdown:
#   8g * 0.6 = 4.8g execution + storage (spark.memory.fraction)
#   8g * 0.4 = 3.2g user memory (UDFs, internal objects)
#   8g * 0.1 = 800MB overhead (JVM, system) (spark.yarn.executor.memoryOverhead)
```

## Deployment Checklist

- [ ] JVM flags set (fixed heap, GC configured)
- [ ] Spark executor sizing validated with a representative dataset
- [ ] Akka cluster seed nodes configured
- [ ] Split brain resolver enabled
- [ ] Health check endpoint exposed
- [ ] Metrics exported to Prometheus or equivalent
- [ ] Logs aggregated (ELK, CloudWatch, or similar)
- [ ] Graceful shutdown handling (finish processing current batch)
- [ ] Retry logic for transient failures (database, network)
- [ ] Alerting on job latency, error rate, and data freshness
