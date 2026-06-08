# What Is Programming `[Entry]`

A program is a set of instructions that tells a computer what to do. Think of it like a recipe.

## The Recipe Analogy

A recipe has:

1. **Ingredients** -- the data you work with (flour, eggs, sugar)
2. **Steps** -- the operations you perform (mix, bake, cool)
3. **Output** -- the result (a cake)

A program works the same way:

```scala
// Ingredients (data)
val eggs = 3
val flourGrams = 250
val sugarGrams = 200

// Steps (operations)
val mixed = eggs + flourGrams + sugarGrams
val baked = "Bake at 180C for 30 minutes"
val result = "Cake is ready"

// Output
println(result)
```

Every program follows this pattern: take input, transform it, produce output.

## What Code Actually Does

At the lowest level, a CPU executes instructions: move data between memory locations, perform arithmetic, compare values, jump to different instructions based on comparisons.

Programming languages exist so you do not have to write CPU instructions directly. You write in human-readable syntax, and a tool converts it to machine instructions.

```scala
val total = items.map(_.price).sum
```

This single line generates hundreds of CPU instructions. The language handles the translation.

## Why This Matters for Data Engineering

Data engineering is programming applied to a specific domain: moving, transforming, and storing data at scale. You write programs that:

- Read data from sources (databases, files, APIs, message queues)
- Transform data (clean, validate, aggregate, join)
- Write data to destinations (data lakes, warehouses, dashboards)

The recipes get more complex -- processing millions of records across dozens of machines -- but the mental model stays the same: input, transform, output.
