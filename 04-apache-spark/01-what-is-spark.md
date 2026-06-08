# What Is Spark ``

Apache Spark is a distributed data processing engine. It splits large datasets across multiple machines, processes them in parallel, and combines the results. Spark is written in Scala and runs on the JVM.

## Why Spark Exists

Before Spark, Hadoop MapReduce was the standard for big data processing. MapReduce wrote intermediate results to disk between every stage. A 4-stage pipeline wrote to disk 3 times. This made MapReduce slow for iterative and interactive workloads.

Spark keeps data in memory between stages. A 4-stage pipeline reads from disk once and writes once. For iterative algorithms (ML training) and interactive queries, Spark is 10-100x faster than MapReduce.

## The Three Abstractions

| Abstraction | Level | When to Use |
|-------------|-------|-------------|
| DataFrames / Spark SQL | High | Most data engineering tasks. Optimized by Catalyst. |
| Datasets | Typed | When you need compile-time type safety on rows. |
| RDDs | Low | Fine-grained control over partitioning. Rare in 2026. |

Use DataFrames. The Catalyst optimizer transforms DataFrame operations into an efficient execution plan. Write readable code, get optimized execution.

## The Mental Model: Driver, Executors, Partitions

```mermaid
graph TD
    Driver["Driver<br/>(SparkSession)"]
    CM["Cluster Manager<br/>(YARN / K8s)"]
    E1["Executor 1<br/>(4 tasks)"]
    E2["Executor 2<br/>(4 tasks)"]
    E3["Executor 3<br/>(4 tasks)"]
    Storage["Storage<br/>(S3 / HDFS)"]

    Driver -->|"1. Submit job"| CM
    CM -->|"2. Allocate"| E1
    CM -->|"2. Allocate"| E2
    CM -->|"2. Allocate"| E3
    E1 -->|"3. Read/Write partitions"| Storage
    E2 -->|"3. Read/Write partitions"| Storage
    E3 -->|"3. Read/Write partitions"| Storage
    E1 <-->|"4. Shuffle"| E2
    E2 <-->|"4. Shuffle"| E3

    style Driver fill:#4a90d9,color:#fff
    style CM fill:#666,color:#fff
    style Storage fill:#d9a84a,color:#000
```

Step by step:

```mermaid
graph TD
    CM[Cluster Manager] --> DRV[Driver Program]
    CM --> EX1[Executor 1\nCache + Tasks]
    CM --> EX2[Executor 2\nCache + Tasks]
    CM --> EX3[Executor 3\nCache + Tasks]
    DRV -->|"split work"| EX1
    DRV -->|"split work"| EX2
    DRV -->|"split work"| EX3
    EX1 <-->|"shuffle"| EX2
    EX2 <-->|"shuffle"| EX3
```

1. **Driver**: Your SparkSession. It converts your code into a logical plan, optimizes it, and splits it into tasks.
2. **Cluster Manager**: Allocates executors on worker nodes. Spark supports YARN, Kubernetes, and its own standalone cluster manager.
3. **Executors**: JVM processes on worker nodes. Each executor runs tasks and stores cached data. Data is split into **partitions** -- each partition is processed by one task on one executor.
4. **Shuffle**: When data needs to be reorganized across partitions (e.g., `groupBy`), executors exchange data over the network. Shuffles are the most expensive operation in Spark.

## A First Spark Job

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

val spark = SparkSession.builder()
  .appName("FirstJob")
  .master("local[*]")  // local mode for development
  .getOrCreate()

import spark.implicits.*

// Read data
val df = spark.read
  .option("header", "true")
  .csv("data/events.csv")

// Transform
val result = df
  .filter(col("event_type").isNotNull)
  .groupBy("event_type")
  .count()
  .orderBy(col("count").desc)

// Show results
result.show()
```

When you run this in local mode, the driver and executor run in the same JVM. When you deploy to a cluster, the driver submits tasks to remote executors. The code does not change.

## Lazy Evaluation

Spark transformations are lazy. Calling `filter`, `groupBy`, or `withColumn` does not execute anything. It builds a logical plan. Execution happens only when you call an **action**: `show`, `collect`, `write`, `count`.

```mermaid
flowchart LR
    DF[DataFrame] --> T1["filter() — plan"]
    T1 --> T2["map() — plan"]
    T2 --> T3["groupBy() — plan"]
    T3 --> ACT["collect() — ACTION!"]
    ACT --> EXEC["Now Spark executes\nthe entire plan"]
```

```scala
val transformed = df
  .filter(col("amount") > 100)    // lazy
  .withColumn("fee", col("amount") * 0.03)  // lazy
  .groupBy("category").sum("fee") // lazy

transformed.show()  // THIS triggers execution
```

Lazy evaluation allows Catalyst to see the entire pipeline and optimize it as a whole. Predicate pushdown, column pruning, and join reordering happen across the entire plan, not stage by stage.
