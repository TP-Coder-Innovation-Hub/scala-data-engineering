# AGENTS.md

Context and guidelines for AI agents contributing to this repository.

## Context

This is the **Scala Data Engineering** learning path, part of TP-Coder Innovation Hub. It teaches Scala 3.x for data engineering, covering the actor model (Akka/Pekko), streaming (Akka Streams), and distributed processing (Apache Spark). The audience spans entry-level engineers learning Scala through senior engineers making architectural decisions.

## Audience

- **Entry** (`[Entry]`): New to Scala or functional programming. Needs clear explanations, minimal jargon, working code examples.
- **Mid** (`[Mid]`): Comfortable with Scala. Needs deeper patterns, performance considerations, framework internals.
- **Senior** (`[Senior]`): Designing systems. Needs architectural trade-offs, decision frameworks, operational knowledge.

Always indicate the target level with `[Entry]`, `[Mid]`, or `[Senior]` badges in headings or exercise descriptions.

## How to Help

- Write Scala 3.x syntax. Do not use Scala 2 syntax (no `implicit` keyword, no `return`, no semicolons, no braces for single-expression methods).
- Use `sbt` for build definitions or Scala CLI for single-file examples. State which tool the example assumes.
- Prefer DataFrames over RDDs in Spark examples. Only show RDDs when explaining low-level concepts.
- Use `cats` and `cats.effect` for functional programming patterns (Monad, Semigroup, IO).
- Use `doobie` for database access examples.
- Use `circe` or `uPickle` for JSON serialization.
- Include working, compilable code. If an example omits imports for brevity, note it.
- Use Mermaid diagrams for architecture, data flow, and type hierarchies.
- Use tables for comparisons and decision frameworks.
- When explaining errors, show the compiler error message and then the fix.

## How NOT to Help

- Do not use Scala 2 syntax (`implicit def`, `implicit val`, `implicit class`). Use Scala 3 `given`/`using`/`extension`.
- Do not suggest Python, Java, or Go as replacements unless the user explicitly asks for a comparison.
- Do not use emojis in content.
- Do not generate placeholder content (e.g., "TODO: add example"). Either write the content or say you need more context.
- Do not use `var` unless the pattern specifically requires mutable state (e.g., inside an actor's receive method). Default to `val`.
- Do not use `null`. Use `Option[A]`.
- Do not use exceptions for control flow. Use `Either[E, A]` or `Try[A]`.
- Do not recommend deprecated APIs. Check version compatibility for Spark 4.x, Akka/Pekko 1.x, Scala 3.x.

## Key Concepts

These concepts recur across modules. Agents should reference them consistently:

- **Immutability**: Default to `val` and immutable collections. Explain when mutability is justified (actors, performance-critical hot paths).
- **Type safety**: Leverage the compiler. Use `Either` for error channels, `Option` for nullable values, enums for sealed hierarchies.
- **For-comprehensions**: The idiomatic way to chain monadic operations. Prefer over nested `flatMap`/`map`.
- **Pattern matching**: Use for data decomposition. Always make matches exhaustive.
- **Backpressure**: Critical concept in streaming. Explain why it matters before showing how.
- **Catalyst optimizer**: Explain what it does when showing DataFrame examples. Users should understand that readable code is optimized code.
- **Supervision**: In actor examples, always define a supervision strategy. Actors without supervision strategies are incomplete.

## Scala Guidelines 2026

| Tool | Version | Notes |
|------|---------|-------|
| Scala | 3.x (3.6+) | Significant indentation, extension methods, given/using, union types |
| sbt | 2.x | Build tool for multi-module projects |
| Scala CLI | Latest | For single-file scripts and prototyping |
| Apache Spark | 4.x | DataFrames primary API, AQE default-on |
| Akka / Pekko | 1.x (Pekko) | Actor model, streams, cluster |
| cats | 2.x | Type classes, monadic combinators |
| cats.effect | 3.x | IO monad, resource management, concurrency |
| doobie | 1.x | Type-safe JDBC via cats.effect |
| circe | 0.14.x | JSON encoding/decoding via type classes |

### Scala 3 Syntax Reference

```scala
// Enums (not sealed trait + case object)
enum EventType:
  case Click, Purchase, PageView, Error

// Extension methods (not implicit class)
extension (s: String)
  def toEventType: Option[EventType] =
    EventType.values.find(_.toString == s)

// Given instances (not implicit val)
given JsonEncoder[EventType] with
  def encode(t: EventType): String = s""""${t.toString}""""

// Using clauses (not implicit parameters)
def writeEvent[A](event: A)(using enc: JsonEncoder[A]): String =
  enc.encode(event)

// Significant indentation (optional braces)
def process(data: List[String]): List[Int] =
  data
    .filter(_.nonEmpty)
    .map(_.length)
```

## Repository Structure

```
scala-data-engineering/
  README.md              # This fundamentals guide (Module 1)
  AGENTS.md              # This file
  modules/
    01-fundamentals/     # Exercises and examples for this guide
    02-streaming-pipeline/ # Kafka-to-Delta-Lake pipeline (Module 2)
    03-type-safe-transforms/ # cats, circe, generic programming (Module 3)
    04-testing/          # Property-based testing, test containers (Module 4)
    05-production/       # Monitoring, schema evolution, data quality (Module 5)
```

When adding content, place it in the appropriate module directory. If no module directory exists yet, create it following the naming convention `NN-topic-name` with a zero-padded module number.
