# Functions ``

Functions are the core building block of Scala. They are first-class: you can assign them to variables, pass them as arguments, and return them from other functions.

## Defining Functions

```scala
// Named function with explicit types
def add(a: Int, b: Int): Int =
  a + b

// Call it
val result = add(3, 4)  // 7
```

The parameter types and return type are declared explicitly for public functions. For local functions, Scala can infer the return type.

## Single-Expression Functions

When a function body is a single expression, omit the `=` and braces:

```scala
def double(x: Int): Int = x * 2

def greet(name: String): String = s"Hello, $name"
```

## Anonymous Functions (Lambdas)

Functions without a name. You pass these to higher-order functions:

```scala
// Full syntax
val addOne = (x: Int) => x + 1

// Placeholder syntax (when each parameter appears once)
val doubled = List(1, 2, 3).map(_ * 2)   // List(2, 4, 6)
val evens = List(1, 2, 3, 4).filter(_ % 2 == 0)  // List(2, 4)
```

Placeholder syntax `_` is idiomatic for simple lambdas. Use full syntax when the logic is complex or when you need to reference a parameter multiple times.

## Higher-Order Functions

Functions that take functions as parameters or return functions:

```scala
// Takes a function as parameter
def transform(numbers: List[Int], f: Int => Int): List[Int] =
  numbers.map(f)

transform(List(1, 2, 3), _ * 10)   // List(10, 20, 30)
transform(List(1, 2, 3), n => n * n) // List(1, 4, 9)

// Returns a function
def multiplier(factor: Int): Int => Int =
  (x: Int) => x * factor

val triple = multiplier(3)
triple(5)  // 15
triple(10) // 30
```

## Default Parameters and Named Arguments

```scala
def connect(host: String, port: Int = 5432, database: String = "analytics"): String =
  s"jdbc:postgresql://$host:$port/$database"

connect("localhost")
// jdbc:postgresql://localhost:5432/analytics

connect("prod-server", database = "warehouse")
// jdbc:postgresql://prod-server:5432/warehouse
```

## Functions Are First-Class

You can store, compose, and pass functions like any other value:

```scala
val square: Int => Int = x => x * x
val negate: Int => Int = x => -x

// Compose: negate(square(x))
val negateSquare = negate.compose(square)
negateSquare(4)  // -16

// AndThen: negate(square(x)) reversed
val squareThenNegate = square.andThen(negate)
squareThenNegate(4)  // -16
```

## Why This Matters for Data Engineering

Data pipelines are chains of functions: read -> parse -> validate -> transform -> write. Each step is a function that takes data in and produces data out. Higher-order functions (`map`, `filter`, `fold`) let you compose these steps declaratively:

```scala
val pipeline = rawEvents
  .map(parse)            // function as argument
  .filter(_.isValid)     // function as argument
  .map(enrich)           // function as argument
  .groupMap(_.category)(_.value)  // two functions as arguments
```

No loops. No mutation. Each step is a pure transformation.
