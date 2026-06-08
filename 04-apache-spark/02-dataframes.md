# DataFrames `[Entry]` `[Mid]`

DataFrames are Spark's primary API for data processing. A DataFrame is a distributed table with named columns and a defined schema -- conceptually similar to a table in a relational database or a DataFrame in pandas, but distributed across a cluster.

## Creating a DataFrame

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import spark.implicits.*

val spark = SparkSession.builder()
  .appName("DataFrameExample")
  .getOrCreate()

// From a file
val events = spark.read.parquet("s3://data-lake/raw/events/")

// From a CSV with schema inference
val csv = spark.read
  .option("header", "true")
  .schema("id STRING, event_type STRING, timestamp LONG, amount DOUBLE")
  .csv("data/events.csv")

// From a Seq of case classes
case class Event(id: String, eventType: String, amount: Double)
val sample = Seq(
  Event("e1", "click", 0.0),
  Event("e2", "purchase", 49.99),
  Event("e3", "purchase", 129.50)
).toDF()
```

## Schema

Every DataFrame has a schema that defines column names and types:

```scala
events.printSchema()
// root
//  |-- id: string (nullable = true)
//  |-- event_type: string (nullable = true)
//  |-- timestamp: long (nullable = true)
//  |-- amount: double (nullable = true)
//  |-- user_id: string (nullable = true)
```

Define schemas explicitly for production code. Schema inference from CSV is convenient but can misinfer types (a numeric column with null values might be inferred as string).

## Core Operations

### Selecting and Renaming

```scala
val selected = events
  .select("id", "event_type", "amount")
  .withColumnRenamed("event_type", "eventType")
  .withColumn("amountUSD", col("amount"))
```

### Filtering

```scala
val purchases = events.filter(col("event_type") === "purchase")
val bigPurchases = events.filter(col("amount") > 100)
val combined = events.filter(
  col("event_type").isIn("purchase", "refund") && col("amount") > 0
)
```

### Adding and Modifying Columns

```scala
val enriched = events
  .withColumn("eventDate", to_date(col("timestamp") / 1000))
  .withColumn("hour", hour(to_timestamp(col("timestamp") / 1000)))
  .withColumn("isHighValue", when(col("amount") > 100, true).otherwise(false))
```

### Aggregations

```scala
val summary = events
  .filter(col("event_type") === "purchase")
  .groupBy("eventDate", "eventType")
  .agg(
    count("*").as("totalEvents"),
    sum("amount").as("totalRevenue"),
    avg("amount").as("avgOrderValue"),
    approx_count_distinct("user_id").as("uniqueUsers")
  )
```

### Joins

```scala
val users = spark.read.parquet("s3://data-lake/users/")
val enriched = events.join(users, events("user_id") === users("id"), "left")
```

Join types: `"inner"`, `"left"`, `"right"`, `"full"`, `"cross"`, `"semi"`, `"anti"`.

### Sorting and Limiting

```scala
val top10 = summary
  .orderBy(col("totalRevenue").desc)
  .limit(10)
```

## Why DataFrames Over RDDs `[Mid]`

DataFrames go through the Catalyst optimizer. RDDs do not.

```scala
// DataFrame: Catalyst optimizes this
df.filter(col("amount") > 100).select("id", "amount")
// Catalyst pushes the filter down (predicate pushdown)
// Catalyst drops unused columns (column pruning)

// RDD: no optimization
rdd.filter(row => row.getDouble(2) > 100).map(row => (row.getString(0), row.getDouble(2)))
```

Catalyst transforms DataFrame operations into an efficient physical plan:

1. **Logical plan**: resolve columns and types against the schema.
2. **Logical optimization**: predicate pushdown, column pruning, constant folding.
3. **Physical planning**: generate physical plans, cost each one, select cheapest.
4. **Code generation**: compile the plan into JVM bytecode at runtime.

Write readable DataFrame code. Catalyst handles the optimization.

## Actions (Trigger Execution)

| Action | Description |
|--------|-------------|
| `show(n)` | Print first n rows to console |
| `collect()` | Bring all rows to the driver (caution: memory) |
| `count()` | Count rows |
| `write.format(...).save(path)` | Write to storage |
| `createOrReplaceTempView(name)` | Register as a SQL view |
| `foreach(f)` | Apply a function to each row (for side effects) |

Transformations are lazy. Actions trigger execution.
