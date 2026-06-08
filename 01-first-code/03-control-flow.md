# Control Flow ``

Scala control flow is expression-oriented. `if`, `match`, and `for` produce values. This is a shift from statement-oriented languages.

## if/else

In Scala, `if`/`else` is an expression that returns a value:

```scala
val count = 15

val label = if count > 100 then "high"
  else if count > 10 then "medium"
  else "low"
// label = "medium"
```

You can assign the result directly to a `val`. No ternary operator needed -- `if`/`else` already does the job.

## match/case (Pattern Matching)

Pattern matching is Scala's most powerful control flow construct. It replaces and extends switch statements:

```scala
enum EventType:
  case Click, Purchase, PageView, Error

def handle(eventType: EventType): String =
  eventType match
    case EventType.Click    => "User engaged"
    case EventType.Purchase => "Revenue event"
    case EventType.PageView => "Browsing"
    case EventType.Error    => "Alert: check logs"
```

The compiler checks exhaustiveness. If you add a new `EventType` later and forget to update a match, the compiler errors. This is critical for data pipelines where unhandled cases cause silent data loss.

Matching on values and conditions:

```scala
def classify(count: Int): String =
  count match
    case 0          => "none"
    case n if n < 5 => "few"
    case n if n < 20 => "moderate"
    case _          => "many"
```

Matching on types:

```scala
def describe(value: Any): String =
  value match
    case s: String  => s"String of length ${s.length}"
    case i: Int     => s"Integer: $i"
    case b: Boolean => s"Boolean: $b"
    case _          => "Unknown type"
```

## for-Comprehensions

for-comprehensions iterate over collections and can filter and transform:

```scala
val numbers = List(1, 2, 3, 4, 5)

// filter + transform
val result = for
  n <- numbers
  if n > 2
yield n * 10
// result = List(30, 40, 50)
```

for-comprehensions also chain operations that produce wrapped values (like `Option`, `Either`, `List`):

```scala
def parseInt(s: String): Option[Int] =
  s.toIntOption

val result = for
  a <- parseInt("10")
  b <- parseInt("20")
yield a + b
// result = Some(30)

val failed = for
  a <- parseInt("10")
  b <- parseInt("abc")   // None
yield a + b
// failed = None
```

When any step produces `None`, the entire comprehension short-circuits. No nested `if` statements needed.

## while

`while` loops exist but are rarely used in idiomatic Scala. Prefer `map`, `filter`, `fold`, or for-comprehensions:

```scala
// Rarely needed: while loop
var i = 0
while i < 5 do
  println(i)
  i += 1

// Preferred: functional iteration
(0 until 5).foreach(println)
```

Use `while` only when you need fine-grained control over loop execution (e.g., reading from a stream until EOF). In data pipelines, you almost never use it.
