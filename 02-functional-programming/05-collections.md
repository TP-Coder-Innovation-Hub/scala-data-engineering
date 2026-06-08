# Collections `[Entry]`

Scala collections are immutable by default. Operations return new collections rather than modifying the original. The three you will use most: `List`, `Map`, and `Vector`.

## List

A singly-linked list. Fast at the front (prepend is O(1)), slow at random access (O(n)).

```scala
val nums = List(1, 2, 3, 4, 5)

// Prepend
val extended = 0 :: nums          // List(0, 1, 2, 3, 4, 5)
val extended2 = 0 +: nums         // same thing

// Append (slower, creates new list)
val appended = nums :+ 6          // List(1, 2, 3, 4, 5, 6)

// Common operations
nums.head          // 1
nums.tail          // List(2, 3, 4, 5)
nums.length        // 5
nums.reverse       // List(5, 4, 3, 2, 1)
nums.take(3)       // List(1, 2, 3)
nums.drop(2)       // List(3, 4, 5)
nums.isEmpty       // false
```

## Vector

An indexed sequence with effectively O(log32(n)) random access. Use `Vector` when you need balanced performance across operations.

```scala
val vec = Vector(1, 2, 3, 4, 5)

// Random access is fast
vec(0)            // 1
vec(4)            // 5

// Updates return a new vector
val updated = vec.updated(2, 99)  // Vector(1, 2, 99, 4, 5)
```

Rule of thumb: use `List` for sequential processing (head/tail recursion). Use `Vector` when you need indexed access or when the access pattern is mixed. For most data pipeline code, `List` is fine.

## Map

Key-value pairs. Immutable by default.

```scala
val scores = Map(
  "alice" -> 95,
  "bob" -> 87,
  "charlie" -> 92
)

// Lookup
scores("alice")            // 95
scores.getOrElse("dave", 0) // 0

// Add/update (returns new map)
val added = scores + ("dave" -> 78)
val removed = scores - "bob"

// Transform values
val curved = scores.view.mapValues(_ + 5).toMap
// Map("alice" -> 100, "bob" -> 92, "charlie" -> 97)

// Iterate
for (name, score) <- scores do
  println(s"$name: $score")

// groupBy: partition a collection into a Map
val events = List("click", "purchase", "click", "click", "error")
val grouped = events.groupBy(identity)
// Map("click" -> List("click", "click", "click"), "purchase" -> List("purchase"), "error" -> List("error"))
```

## Set

A collection of unique elements.

```scala
val tags = Set("scala", "spark", "data")
val more = tags + "streaming"       // Set("scala", "spark", "data", "streaming")
val overlap = tags & Set("spark", "python")  // Set("spark") -- intersection
```

## Common Operations Across All Collections

| Operation | Description | Example |
|-----------|-------------|---------|
| `map(f)` | Transform each element | `List(1,2,3).map(_ * 2)` |
| `filter(p)` | Keep matching elements | `List(1,2,3).filter(_ > 1)` |
| `foldLeft(z)(f)` | Accumulate into a single value | `List(1,2,3).foldLeft(0)(_ + _)` |
| `flatMap(f)` | Transform and flatten | `List(1,2).flatMap(n => List(n, n*10))` |
| `find(p)` | First element matching predicate | `List(1,2,3).find(_ > 2)` |
| `exists(p)` | True if any element matches | `List(1,2,3).exists(_ == 2)` |
| `forall(p)` | True if all elements match | `List(2,4,6).forall(_ % 2 == 0)` |
| `groupBy(f)` | Partition by key into a Map | `List(1,2,3).groupBy(_ % 2)` |
| `sorted` | Sort elements | `List(3,1,2).sorted` |
| `distinct` | Remove duplicates | `List(1,1,2).distinct` |
| `zip(other)` | Pair elements with another collection | `List(1,2).zip(List("a","b"))` |

## Immutable Returns New

Every operation returns a new collection. The original is never modified:

```scala
val original = List(1, 2, 3)
val modified = original.map(_ * 2)

original  // still List(1, 2, 3)
modified  // List(2, 4, 6)
```

This is the foundation of safe data pipelines. Each transformation step produces a new dataset. You can inspect any intermediate result without affecting the pipeline.
