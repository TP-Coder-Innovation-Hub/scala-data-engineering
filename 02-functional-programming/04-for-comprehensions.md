# For-Comprehensions `` ``

For-comprehensions are Scala's syntax for chaining operations on wrapped values (collections, `Option`, `Either`, `Future`). They are not loops -- they are syntactic sugar for `map`, `flatMap`, and `filter`. Scala developers use them constantly.

## Basic Syntax

```scala
val numbers = List(1, 2, 3, 4, 5)

val result = for
  n <- numbers
  if n > 2
yield n * 10
// result = List(30, 40, 50)
```

This desugars to:

```scala
numbers.filter(_ > 2).map(_ * 10)
```

The `<-` (generator) pulls values from a collection. `if` filters. `yield` produces the output.

## Multiple Generators

```scala
val pairs = for
  x <- List(1, 2, 3)
  y <- List("a", "b")
yield s"$x-$y"
// List("1-a", "1-b", "2-a", "2-b", "3-a", "3-b")
```

This desugars to nested `flatMap` calls:

```scala
List(1, 2, 3).flatMap(x => List("a", "b").map(y => s"$x-$y"))
```

The for-comprehension is far more readable than the nested `flatMap` version. Use it.

## With Option: Chaining Operations That Can Fail

```scala
def findUser(id: String): Option[String] =
  if id.startsWith("u") then Some(s"User($id)") else None

def getEmail(user: String): Option[String] =
  if user.nonEmpty then Some(s"$user@example.com") else None

// Imperative style (nested, hard to read)
val result1 = findUser("u42") match
  case Some(user) => getEmail(user)
  case None       => None

// For-comprehension (flat, readable)
val result2 = for
  user  <- findUser("u42")
  email <- getEmail(user)
yield email
// result2 = Some("User(u42)@example.com")
```

When any step returns `None`, the entire comprehension short-circuits to `None`. No nested `if` or `match` needed.

## With Either: Chaining with Error Messages

```scala
def parseId(raw: String): Either[String, Int] =
  raw.toIntOption.toRight(s"Not a number: $raw")

def validateAge(age: Int): Either[String, Int] =
  if age >= 0 then Right(age) else Left(s"Negative age: $age")

val result = for
  id  <- parseId("42")
  age <- validateAge(25)
yield s"User $id, age $age"
// Right("User 42, age 25")

val failed = for
  id  <- parseId("abc")
  age <- validateAge(25)
yield s"User $id, age $age"
// Left("Not a number: abc")
```

This is how data validation pipelines work in Scala: chain `Either` values in a for-comprehension. The first failure stops the chain and returns the error.

## Without yield (Imperative Style)

When you omit `yield`, the for-comprehension executes side effects:

```scala
for
  line <- scala.io.Source.fromFile("data.csv").getLines()
  if line.nonEmpty
do println(line)
```

Use this for iteration with side effects. Prefer the `yield` version when producing values.

## Why Scala Developers Love Them ``

For-comprehensions work with any type that has `map` and `flatMap` methods:

| Type | Behavior |
|------|----------|
| `List[A]` | Produces a list of results |
| `Option[A]` | Short-circuits on `None` |
| `Either[E, A]` | Short-circuits on `Left` |
| `Future[A]` | Chains asynchronous operations |
| Spark `DataFrame` | Chains transformations (lazy) |

This uniformity means you learn one syntax that works across different contexts. A data validation chain with `Either` uses the same syntax as a collection transformation with `List`. The mental model transfers.

In data pipelines, for-comprehensions are the primary tool for chaining validation, parsing, and transformation steps where each step can fail.
