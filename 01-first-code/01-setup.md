# Setup ``

Get a Scala development environment running. You need three things: a JDK, a build tool, and an editor.

## Step 1: Install the JDK

Scala runs on the JVM. Install JDK 21 (LTS):

```bash
# macOS (Homebrew)
brew install openjdk@21

# Linux (sdkman)
curl -s "https://get.sdkman.io" | bash
sdk install java 21.0.4-tem

# Verify
java -version
# openjdk version "21.0.4"
```

Set `JAVA_HOME` to point to your JDK installation.

## Step 2: Install a Build Tool

You have two options:

**sbt** -- for multi-module projects (what you will use in production):

```bash
brew install sbt    # macOS
sdk install sbt     # Linux via sdkman

# Verify
sbt --version
# sbt 2.x
```

**Scala CLI** -- for single-file scripts and quick experiments:

```bash
brew install scala-cli    # macOS

# Verify
scala-cli --version
```

Use Scala CLI for learning and prototyping. Use sbt for real projects.

## Step 3: Choose an Editor

- **VS Code** with the Scala (Metals) extension. Best free option.
- **IntelliJ IDEA** with the Scala plugin. Most feature-complete.
- **Vim/Neovim** with Metals. For terminal-oriented developers.

Install Metals (the Scala language server) -- it provides autocomplete, go-to-definition, inline errors, and refactoring regardless of which editor you choose.

## Step 4: Run Your First Program

Create a file:

```scala
// hello.scala
// Run with: scala-cli run hello.scala

@main def hello(): Unit =
  val name = "Scala"
  val version = 3
  println(s"Hello, $name $version!")
  println(s"2 + 2 = ${2 + 2}")
  println("Data engineering starts here.")
```

Run it:

```bash
scala-cli run hello.scala
# Hello, Scala 3!
# 2 + 2 = 4
# Data engineering starts here.
```

## Step 5: Create an sbt Project (Optional)

For anything beyond single files, use sbt:

```bash
sbt new scala/scala3.g8
cd scala3-project
sbt run
```

The project structure:

```
scala3-project/
  build.sbt           # dependencies and settings
  project/
    build.properties   # sbt version
  src/
    main/
      scala/
        Main.scala     # your code
```

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `java: command not found` | Install JDK, set JAVA_HOME |
| `sbt: command not found` | Install sbt, restart terminal |
| `sbt` hangs downloading | Check network, clear `~/.sbt/boot` |
| Metals not connecting | Run `sbt compile` first, then restart Metals |
