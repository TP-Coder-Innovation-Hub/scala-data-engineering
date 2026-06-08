# Sequential, Decision, Iteration ``

Every program, regardless of language, is built from three constructs: sequential execution, conditional decisions, and iterative loops. Master these and you can express any computation.

## Sequential: Do This, Then This

Statements execute in order, top to bottom.

```scala
val raw = readFile("events.csv")
val parsed = parseCsv(raw)
val filtered = parsed.filter(_.nonEmpty)
writeOutput(filtered, "output.parquet")
```

Each line depends on the result of the previous one. This is the default flow of any program.

## Decision: Do This OR That Based on a Condition

Programs need to make choices. In Scala, the primary decision constructs are `if`/`else` expressions and `match` expressions.

```scala
// if/else -- note: in Scala, this is an EXPRESSION that returns a value
val label = if count > 1000 then "high" else "low"

// match/case -- Scala's powerful pattern matching
def categorize(eventType: String): String =
  eventType match
    case "click"     => "engagement"
    case "purchase"  => "revenue"
    case "error"     => "alert"
    case other       => s"unknown: $other"
```

Scala's `if` and `match` are expressions, not statements. They produce values. This means you can assign the result directly to a `val`, pass it to a function, or return it. This is a shift from languages like Java or Python where `if` is a statement that controls flow but does not produce a value.

## Iteration: Do This REPEATEDLY

Programs need to process collections of items. Scala provides several iteration mechanisms:

```scala
val numbers = List(1, 2, 3, 4, 5)

// map -- transform each element
val doubled = numbers.map(n => n * 2)
// List(2, 4, 6, 8, 10)

// filter -- keep elements matching a condition
val evens = numbers.filter(n => n % 2 == 0)
// List(2, 4)

// foldLeft -- accumulate a result
val sum = numbers.foldLeft(0)((acc, n) => acc + n)
// 15

// for-comprehension -- readable iteration
val results = for
  n <- numbers
  if n > 2
yield n * 10
// List(30, 40, 50)
```

In functional Scala, you rarely write raw `while` loops. You use higher-order functions (`map`, `filter`, `fold`) or for-comprehensions instead. These are safer because they avoid mutable loop variables and off-by-one errors.

## Combining the Three

Real programs interleave all three constructs:

```scala
def processEvents(events: List[String]): List[String] =
  events                                 // iteration
    .filter(_.nonEmpty)                  // decision: keep non-empty
    .map: event =>                       // iteration + decision
      if event.startsWith("error") then  // decision
        s"ALERT: $event"
      else
        s"OK: $event"
```

Sequential, decision, iteration. Every program reduces to these three building blocks, composed in layers.
