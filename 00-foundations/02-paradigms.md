# Programming Paradigms `[Entry]`

A paradigm is a way of thinking about how to structure code. The two dominant paradigms are object-oriented programming (OOP) and functional programming (FP). Scala supports both. For data engineering, FP is the stronger tool.

## Object-Oriented Programming

OOP organizes code around objects: data structures that bundle state (fields) and behavior (methods). Objects communicate by calling methods on each other.

```scala
class EventProcessor:
  private var eventCount: Int = 0

  def process(event: String): String =
    eventCount += 1
    s"Processed event #$eventCount: $event"

val processor = EventProcessor()
processor.process("user_clicked")
processor.process("page_viewed")
```

OOP works well for modeling real-world entities with complex lifecycle and state. UI frameworks, game engines, and business domain models often benefit from OOP.

## Functional Programming

FP organizes code around functions: transformations that take input and produce output without modifying external state. Data flows through a pipeline of pure functions.

```scala
val events = List("user_clicked", "page_viewed", "user_clicked")

val processed = events
  .map(event => event.toUpperCase)
  .filter(_.contains("CLICK"))
  .groupBy(identity)
  .view.mapValues(_.length).toMap

// processed: Map("USER_CLICKED" -> 2)
```

Key principles:

- **Immutability**: Data structures cannot be modified after creation. You create new data from old data.
- **Pure functions**: Functions that always return the same output for the same input, with no side effects. No modifying global variables, no I/O inside the function.
- **Composition**: Small functions combine into larger ones. `map`, `filter`, `fold` are building blocks you chain together.

## Why FP Is Scala's Strength for Data

Data engineering is transformation: read raw data, validate it, reshape it, aggregate it, write the result. This maps directly onto FP:

```scala
val result = rawData
  .filter(isValid)          // keep valid records
  .map(enrich)              // add derived fields
  .groupBy(_.category)      // group by dimension
  .mapValues(_.map(_.value).sum)  // aggregate
```

Each step is a pure function. The data flows through a pipeline. No mutation. No surprises. Easy to test each step independently.

Scala embraces FP while still allowing OOP where it makes sense. You use FP for data transformations. You use OOP for structuring modules and defining interfaces. This fusion is Scala's unique strength.
