# Spark SQL `[Entry]` `[Mid]`

Spark SQL lets you write SQL queries against DataFrames. Register a DataFrame as a temporary view, then query it with SQL. For analysts and data engineers who think in SQL, this is the fastest path to working with Spark.

## Register and Query

```scala
val events = spark.read.parquet("s3://data-lake/raw/events/")

// Register as a temporary view
events.createOrReplaceTempView("events")

// Query with SQL
val result = spark.sql("""
  SELECT
    event_type,
    COUNT(*) as event_count,
    AVG(amount) as avg_amount,
    MIN(timestamp) as first_seen,
    MAX(timestamp) as last_seen
  FROM events
  WHERE amount > 0
  GROUP BY event_type
  ORDER BY event_count DESC
""")

result.show()
```

The result is a DataFrame. You can continue chaining DataFrame operations after `spark.sql()`:

```scala
spark.sql("SELECT * FROM events WHERE event_type = 'purchase'")
  .withColumn("fee", col("amount") * 0.03)
  .write.parquet("s3://data-lake/processed/purchases/")
```

## Global Temporary Views

`createOrReplaceTempView` is scoped to the current SparkSession. For cross-session access, use global temporary views:

```scala
events.createOrReplaceGlobalTempView("global_events")

spark.sql("SELECT * FROM global_temp.global_events LIMIT 10").show()
```

## When to Use SQL vs DataFrame API

| Factor | SQL | DataFrame API |
|--------|-----|---------------|
| Familiarity | Analysts know SQL | Developers prefer code |
| Complex logic | Window functions, CTEs read naturally in SQL | Complex chains of `.withColumn` become verbose |
| Dynamic queries | Build SQL strings | Compose transformations programmatically |
| IDE support | String-based (no autocomplete) | Type-safe column references |
| Integration | BI tools speak SQL | Programmatic pipelines |

Rule of thumb: use SQL for analytical queries, reporting, and one-off investigations. Use the DataFrame API for production ETL pipelines where you need programmatic control, type safety, and composability.

## Window Functions

```scala
spark.sql("""
  SELECT
    user_id,
    event_type,
    amount,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) as rn,
    SUM(amount) OVER (PARTITION BY user_id ORDER BY timestamp
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total,
    LAG(amount, 1) OVER (PARTITION BY user_id ORDER BY timestamp) as prev_amount
  FROM events
  WHERE event_type = 'purchase'
""")
```

Window functions are one area where SQL is significantly more readable than the DataFrame API equivalent.

## Common Table Expressions (CTEs)

```scala
spark.sql("""
  WITH daily_summary AS (
    SELECT
      to_date(timestamp) as event_date,
      event_type,
      COUNT(*) as event_count,
      SUM(amount) as daily_revenue
    FROM events
    GROUP BY to_date(timestamp), event_type
  ),
  ranked AS (
    SELECT
      *,
      RANK() OVER (PARTITION BY event_date ORDER BY daily_revenue DESC) as revenue_rank
    FROM daily_summary
  )
  SELECT * FROM ranked WHERE revenue_rank <= 3
""")
```

CTEs break complex queries into named steps. Each step is a query you can reason about independently. Use them for multi-stage transformations that would be deeply nested in the DataFrame API.

## SQL Functions

Spark SQL supports standard SQL functions plus Spark-specific ones:

| Function | Description |
|----------|-------------|
| `coalesce(a, b)` | Return first non-null |
| `cast(col AS type)` | Type casting |
| `date_format(date, fmt)` | Format dates |
| `datediff(end, start)` | Days between dates |
| `regexp_extract(str, pattern, idx)` | Extract regex group |
| `json_tuple(json, field1, field2)` | Extract JSON fields |
| `percentile_approx(col, 0.5)` | Approximate median |
| `collect_list(col)` | Aggregate into array |
