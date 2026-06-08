# Scala Data Engineering

Build robust, type-safe data pipelines and distributed systems with Scala 3.x, the Actor Model, and Apache Spark.

## Objectives

- Write idiomatic Scala 3.x with functional programming as the default paradigm
- Think in actors: independent units of computation communicating via messages, no shared state
- Process data at scale with Apache Spark DataFrames and SQL
- Deploy production pipelines with testing, monitoring, and JVM tuning

## Navigation

| Module | Topic | Key Concepts |
|--------|-------|--------------|
| [00-foundations](00-foundations/) | Programming Fundamentals | What code does, FP paradigm, building blocks, Scala history |
| [01-first-code](01-first-code/) | First Scala Programs | Setup, variables, control flow, functions, case classes |
| [02-functional-programming](02-functional-programming/) | Functional Programming | Immutability, pattern matching, HOFs, for-comprehensions, collections |
| [03-actor-model](03-actor-model/) | The Actor Model | Actors, Akka, supervision, Akka Streams with backpressure |
| [04-apache-spark](04-apache-spark/) | Apache Spark | Distributed processing, DataFrames, Spark SQL, streaming |
| [05-production](05-production/) | Production Systems | Testing, monitoring, deployment, JVM tuning |
| [06-capstone](06-capstone/) | Capstone Project | End-to-end data pipeline: ingest, transform, serve |

## Mental Model

This roadmap has three pillars:

1. **Functional Programming** -- immutable data, pure functions, composition. The default way to write Scala.
2. **Actor Model** -- independent units of computation communicating via message passing. Unique to Scala/Akka. No shared state, no locks, no race conditions.
3. **Apache Spark** -- distributed data processing on a cluster. Driver, executors, partitions. DataFrames optimized by Catalyst.

These three pillars reinforce each other: FP gives you correct single-node code, actors give you correct concurrent code, Spark gives you correct distributed code.

## Audience

| Level | Badge | You Should |
|-------|-------|------------|
| Entry | `[Entry]` | Know basic programming. New to Scala or FP. |
| Mid | `[Mid]` | Write Scala comfortably. Understand map/flatMap. |
| Senior | `[Senior]` | Design distributed systems. Make architectural decisions. |

---

*Part of the TP-Coder Innovation Hub learning paths.*
