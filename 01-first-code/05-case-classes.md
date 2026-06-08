# Case Classes `[Entry]`

Case classes are Scala's primary tool for modeling data. They are immutable data containers with built-in methods for comparison, pattern matching, and copying. If you are building a data pipeline, most of your types will be case classes.

## Defining a Case Class

```scala
case class Event(
  id: String,
  eventType: String,
  timestamp: Long,
  userId: String,
  payload: String
)
```

That one line gives you:

1. An immutable constructor (all fields are `val`)
2. Automatic `equals` and `hashCode` (compared by value, not reference)
3. Automatic `toString` (readable output)
4. A `copy` method (create modified versions)
5. Pattern matching support (extract fields in match expressions)
6. Serializable by default

## Creating and Using

```scala
val event = Event(
  id = "evt-001",
  eventType = "click",
  timestamp = 1718000000000L,
  userId = "user-42",
  payload = """{"page":"/home"}"""
)

// Access fields
event.id          // "evt-001"
event.eventType   // "click"

// toString
println(event)
// Event(evt-001,click,1718000000000,user-42,{"page":"/home"})

// Equality by value
val event2 = Event("evt-001", "click", 1718000000000L, "user-42", """{"page":"/home"}""")
event == event2   // true
```

## The Copy Method

Because case classes are immutable, you cannot modify fields. Instead, you create a copy with changed fields:

```scala
val enriched = event.copy(
  eventType = event.eventType.toUpperCase,
  payload = """{"page":"/home","source":"web"}"""
)
```

The `copy` method creates a new instance with the specified fields changed. All other fields keep their original values. This is how you "modify" immutable data.

## Pattern Matching with Case Classes

Case classes work directly in match expressions. The compiler destructures the fields:

```scala
def route(event: Event): String =
  event match
    case Event(_, "click", _, userId, _)    => s"Click from $userId"
    case Event(_, "purchase", _, _, _)       => "Revenue event"
    case Event(id, "error", _, _, payload)   => s"Error $id: $payload"
    case Event(_, eventType, _, _, _)        => s"Unhandled: $eventType"
```

The compiler checks exhaustiveness if you use a sealed hierarchy. Add a new event type, and the compiler forces you to handle it everywhere.

## Nested Case Classes

Model complex data by nesting case classes:

```scala
case class Address(city: String, country: String)
case class User(id: String, name: String, address: Address)

val user = User("u1", "Alice", Address("Berlin", "DE"))

// Destructure in pattern matching
user match
  case User(_, name, Address(city, "DE")) => s"$name from $city, Germany"
  case User(_, name, _)                   => s"$name from elsewhere"
```

## Enums with Data (Scala 3)

Scala 3 enums combine with case classes for sealed data models:

```scala
enum Result:
  case Success(data: String, recordsProcessed: Int)
  case Failure(message: String, cause: Option[String])

def describe(result: Result): String =
  result match
    case Result.Success(data, count)    => s"Processed $count records"
    case Result.Failure(msg, Some(why)) => s"Failed: $msg because $why"
    case Result.Failure(msg, None)      => s"Failed: $msg"
```

## Why Case Classes Dominate in Data Engineering

Data pipelines process records. Records have fields. Case classes give you typed, immutable records with pattern matching and safe copying. Every data model in a Scala pipeline -- raw events, validated events, aggregated results, error records -- is a case class.
