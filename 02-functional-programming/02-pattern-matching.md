# Pattern Matching `` ``

Pattern matching is Scala's mechanism for inspecting and decomposing data. It goes far beyond a switch statement: it matches on types, values, structures, and nested patterns. The compiler checks that matches are exhaustive.

## Basic Matching

```scala
def httpStatus(code: Int): String =
  code match
    case 200 => "OK"
    case 201 => "Created"
    case 301 => "Moved Permanently"
    case 404 => "Not Found"
    case 500 => "Internal Server Error"
    case _   => "Unknown"
```

The `_` is a wildcard that matches anything. Use it as a catch-all at the end of a match.

## Matching on Types

```scala
def format(value: Any): String =
  value match
    case s: String  => s"String: $s"
    case i: Int     => s"Int: $i"
    case d: Double  => s"Double: $d"
    case b: Boolean => s"Boolean: $b"
    case list: List[?] => s"List with ${list.length} elements"
    case _          => "Unknown"
```

## Matching Case Classes (Destructuring)

```scala
case class Event(id: String, eventType: String, userId: String, amount: Double)

def summarize(event: Event): String =
  event match
    case Event(_, "purchase", userId, amount) if amount > 100 =>
      s"High-value purchase by $userId: $$ $amount"
    case Event(_, "purchase", userId, _) =>
      s"Purchase by $userId"
    case Event(id, "click", _, _) =>
      s"Click event $id"
    case Event(id, eventType, _, _) =>
      s"Event $id of type $eventType"
```

The pattern `Event(_, "purchase", userId, amount)` extracts `userId` and `amount` while matching `eventType` against the literal `"purchase"`. The `if` guard adds an additional condition.

## Exhaustiveness Checking

```scala
enum EventType:
  case Click, Purchase, PageView, Error

def handle(et: EventType): String =
  et match
    case EventType.Click => "click"
  // compiler warning: match may not be exhaustive
  // missing case: Purchase, PageView, Error
```

The compiler tells you what you missed. Fix it:

```scala
def handle(et: EventType): String =
  et match
    case EventType.Click    => "click"
    case EventType.Purchase => "purchase"
    case EventType.PageView => "pageview"
    case EventType.Error    => "error"
```

This is critical for data pipelines. When you add a new event type, the compiler forces you to handle it in every match expression. No silent data drops.

## Nested Pattern Matching

```scala
case class Address(city: String, country: String)
case class User(id: String, name: String, address: Option[Address])

def location(user: User): String =
  user match
    case User(_, _, Some(Address(city, "DE"))) => s"Germany: $city"
    case User(_, _, Some(Address(city, "US"))) => s"USA: $city"
    case User(_, _, Some(Address(city, cc)))   => s"$cc: $city"
    case User(_, name, None)                   => s"$name: location unknown"
```

## Pattern Matching in for-Comprehensions

```scala
val results: List[Either[String, Int]] = List(Right(1), Left("error"), Right(3))

val successes = for
  Right(value) <- results   // filters out Left values
yield value
// successes = List(1, 3)
```

Using a pattern in `<-` filters elements that do not match. This is a concise way to extract successful results from a mixed list.

## Sealed Traits and Exhaustiveness ``

When you define a sealed trait, all subtypes must be in the same file. The compiler uses this to check exhaustiveness:

```scala
sealed trait ParseResult
object ParseResult:
  case class Success(data: String) extends ParseResult
  case class Failure(error: String, line: Int) extends ParseResult

def describe(result: ParseResult): String =
  result match
    case ParseResult.Success(data)           => s"OK: $data"
    case ParseResult.Failure(error, line)    => s"Error at line $line: $error"
```

Use sealed traits for your domain types. The compiler becomes your safety net.
