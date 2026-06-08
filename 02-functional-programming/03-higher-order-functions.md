# Higher-Order Functions `[Entry]` `[Mid]`

A higher-order function (HOF) takes a function as a parameter, returns a function, or both. HOFs are how you compose behavior in Scala. The three you will use constantly are `map`, `filter`, and `fold`.

## map: Transform Each Element

`map` applies a function to every element and returns a new collection:

```scala
val numbers = List(1, 2, 3, 4, 5)

val doubled = numbers.map(_ * 2)
// List(2, 4, 6, 8, 10)

val names = List("alice", "bob")
val upper = names.map(_.toUpperCase)
// List("ALICE", "BOB")

// Transform case classes
case class Event(id: String, amount: Double)
val events = List(Event("e1", 10.0), Event("e2", 25.0))
val ids = events.map(_.id)
// List("e1", "e2")
```

## filter: Keep Elements Matching a Condition

```scala
val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

val evens = numbers.filter(_ % 2 == 0)
// List(2, 4, 6, 8, 10)

val bigEvents = events.filter(_.amount > 15.0)
// List(Event("e2", 25.0))

// filterNot: keep elements that DO NOT match
val odds = numbers.filterNot(_ % 2 == 0)
// List(1, 3, 5, 7, 9)
```

## foldLeft: Accumulate a Result

`foldLeft` reduces a collection to a single value by applying a function that takes an accumulator and the current element:

```scala
val numbers = List(1, 2, 3, 4, 5)

val sum = numbers.foldLeft(0)((acc, n) => acc + n)
// 15

// Building a map from a list
val events = List(("click", 1), ("purchase", 2), ("click", 3))
val byType = events.foldLeft(Map.empty[String, List[Int]]) { (acc, pair) =>
  val (eventType, id) = pair
  acc.updated(eventType, acc.getOrElse(eventType, List.empty) :+ id)
}
// Map("click" -> List(1, 3), "purchase" -> List(2))
```

Step by step for `foldLeft(0)` on `List(1, 2, 3)`:

```
acc=0, n=1 -> 0 + 1 = 1
acc=1, n=2 -> 1 + 2 = 3
acc=3, n=3 -> 3 + 3 = 6
result = 6
```

## Chaining HOFs

The power comes from chaining:

```scala
case class Record(id: String, category: String, value: Double, valid: Boolean)

val records = List(
  Record("r1", "A", 10.0, true),
  Record("r2", "B", -5.0, false),
  Record("r3", "A", 20.0, true),
  Record("r4", "A", 30.0, true)
)

val result = records
  .filter(_.valid)                    // keep valid records
  .filter(_.category == "A")          // keep category A
  .map(_.value)                       // extract values
  .foldLeft(0.0)(_ + _)              // sum them
// result = 60.0
```

Each step is a pure function. The data flows through a pipeline. You can read it top to bottom and understand exactly what happens.

## Functions That Return Functions `[Mid]`

HOFs also create configurable behavior:

```scala
def thresholdChecker(minValue: Double): Record => Boolean =
  record => record.value >= minValue

val isHighValue = thresholdChecker(20.0)
val isAnyValue = thresholdChecker(0.0)

records.filter(isHighValue)  // only records with value >= 20
records.filter(isAnyValue)   // all records
```

## Why This Matters

Data engineering is transformation chains. HOFs give you the vocabulary:

- `map` -- transform each record
- `filter` -- keep relevant records
- `foldLeft` -- aggregate records into a result
- `flatMap` -- transform and flatten (one-to-many)
- `groupBy` -- partition by key

Every Spark DataFrame operation, every Akka Streams flow, every collection transformation uses these patterns. Learn them once, use them everywhere.
