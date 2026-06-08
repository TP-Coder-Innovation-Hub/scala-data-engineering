# Variables and Types `[Entry]`

Scala has two kinds of variables: `val` (immutable) and `var` (mutable). Use `val` by default.

## val vs var

```scala
// val: immutable reference. Cannot be reassigned.
val name: String = "Scala"
// name = "Python"  // compile error: reassignment to val

// var: mutable reference. Can be reassigned.
var counter: Int = 0
counter = 1   // ok
counter = 2   // ok
```

Rule: use `val` unless you have a specific reason to use `var`. Immutable references make code easier to reason about because the value never changes after initialization. In data pipelines, every variable should be a `val`.

## Type Inference

Scala can infer types, so you often omit them:

```scala
val x = 42              // Int
val pi = 3.14           // Double
val greeting = "hello"  // String
val flag = true         // Boolean
val nothing = ()        // Unit

// Explicit types when inference is ambiguous or for documentation
val eventId: String = "evt-001"
val count: Long = 1_000_000L
```

The compiler infers types at compile time. This is not dynamic typing -- every variable has a fixed type known to the compiler. You add explicit type annotations for public APIs, complex expressions, and when the inferred type is not specific enough.

## Basic Types

| Type | Description | Example |
|------|-------------|---------|
| `Int` | 32-bit integer | `42` |
| `Long` | 64-bit integer | `1_000_000L` |
| `Double` | 64-bit floating point | `3.14` |
| `Float` | 32-bit floating point | `3.14f` |
| `Boolean` | true or false | `true` |
| `String` | text | `"hello"` |
| `Char` | single character | `'A'` |
| `Unit` | no meaningful value | `()` |

## Strings

Scala strings support string interpolation:

```scala
val user = "Alice"
val age = 30

// s-interpolation: inject expressions
println(s"$user is $age years old")
println(s"Next year: ${age + 1}")

// raw-interpolation: no escape processing
println(raw"No newline here: \n")

// f-interpolation: formatted output
println(f"Pi is approximately $pi%.2f")
```

## Option: No More Null

Scala does not use `null` for missing values. It uses `Option[A]`:

```scala
// A value that might be absent
val present: Option[String] = Some("hello")
val absent: Option[String] = None

// Accessing safely
val result = present.getOrElse("default")  // "hello"
val fallback = absent.getOrElse("default") // "default"

// Transforming options
val upper = present.map(_.toUpperCase)  // Some("HELLO")
val missing = absent.map(_.toUpperCase) // None
```

`Option` forces you to handle the absence case. The compiler will warn you if you ignore it. This eliminates `NullPointerException` from your code.

## Why val by Default

```scala
// BAD: mutable, hard to trace
var total = 0
for item <- items do
  total += item.price

// GOOD: immutable, each step is clear
val total = items.map(_.price).sum
```

The `val` version is a single expression. No loop, no mutation, no intermediate state to track. You can read it left-to-right and understand exactly what it does.
