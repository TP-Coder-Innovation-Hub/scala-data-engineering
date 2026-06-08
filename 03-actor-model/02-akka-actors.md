# Akka Actors `` ``

Akka (now Apache Pekko for the open-source fork) implements the actor model on the JVM. This guide shows how to create actors, send messages, and manage their lifecycle.

## Step 1: Define Messages

Messages must be immutable. Use case classes and case objects:

```scala
object EventProcessor:
  // Commands: messages sent TO the actor
  case class ProcessEvent(eventId: String, payload: String)
  case object GetStatus
  case object Stop

  // Responses: messages sent FROM the actor
  case class Status(processed: Long, errors: Long)
  case class ProcessingResult(eventId: String, success: Boolean)
```

Messages go in the actor's companion object. This keeps them organized and makes the actor's protocol explicit.

## Step 2: Define the Actor

```scala
import org.apache.pekko.actor.*

class EventProcessor extends Actor:
  import EventProcessor.*

  // Private state -- only this actor can access it
  private var processedCount: Long = 0
  private var errorCount: Long = 0

  // Define how the actor responds to messages
  def receive: Receive =
    case ProcessEvent(id, payload) =>
      // Process the event
      if payload.nonEmpty then
        processedCount += 1
        sender() ! ProcessingResult(id, success = true)
      else
        errorCount += 1
        sender() ! ProcessingResult(id, success = false)

    case GetStatus =>
      sender() ! Status(processedCount, errorCount)

    case Stop =>
      context.stop(self)
```

Step by step:

1. Extend `Actor` and implement `receive`.
2. Use pattern matching on messages (case classes from the companion object).
3. Use `sender()` to get a reference to whoever sent the message.
4. Use `!` (pronounced "tell") to send a message. `sender() ! response` sends `response` back.
5. Use `self` to reference the actor itself. `context.stop(self)` stops the actor.

## Step 3: Create and Use the Actor

```scala
import org.apache.pekko.actor.*

object Main extends App:
  // Every Akka application has an ActorSystem
  val system = ActorSystem("DataPipeline")

  // Create an actor. Props holds configuration.
  val processor = system.actorOf(
    Props[EventProcessor](),
    name = "event-processor"
  )

  // Send messages using !
  processor ! ProcessEvent("evt-001", """{"page":"/home"}""")
  processor ! ProcessEvent("evt-002", "")
  processor ! ProcessEvent("evt-003", """{"page":"/about"}""")
  processor ! GetStatus
  processor ! Stop

  // Shut down the system
  system.terminate()
```

`system.actorOf(Props[EventProcessor](), "event-processor")` creates a new actor instance with the given name. The name is used for logging and for looking up the actor in the hierarchy.

## Actor Lifecycle

An actor goes through lifecycle events you can hook into:

```scala
class LifecycleActor extends Actor:
  override def preStart(): Unit =
    println("Actor starting. Initialize resources here.")

  override def postStop(): Unit =
    println("Actor stopped. Clean up resources here.")

  override def preRestart(reason: Throwable, message: Option[Any]): Unit =
    println(s"Actor restarting due to: ${reason.getMessage}")
    super.preRestart(reason, message)  // calls postStop, then preStart

  override def postRestart(reason: Throwable): Unit =
    println("Actor restarted with fresh state.")
    super.postRestart(reason)

  def receive: Receive =
    case msg => println(s"Received: $msg")
```

| Hook | When It Runs | Use For |
|------|-------------|---------|
| `preStart` | After actor is created, before first message | Open connections, initialize state |
| `postStop` | After actor is stopped | Close connections, release resources |
| `preRestart` | Before actor restarts after failure | Save state, log the failure |
| `postRestart` | After actor restarts | Reinitialize state |

## Rules for Safe Actor Code

1. **Never close over mutable state in a Future inside an actor.** If you need async work, pipe the result back as a message:

```scala
// WRONG: closing over mutable state
case ProcessEvent(event) =>
  Future {
    processedCount += 1  // race condition!
  }

// RIGHT: pipe result back as message
case ProcessEvent(event) =>
  val result = Future { processEvent(event) }
  result.pipeToSelf(self):
    case Success(r) => ProcessComplete(r)
    case Failure(e) => ProcessFailed(e)
```

2. **Never block inside receive.** Blocking I/O (JDBC, HTTP, file reads) blocks the actor's thread, which is shared with other actors. Use a dedicated dispatcher for blocking operations.

3. **Messages must be immutable.** Case classes and case objects are immutable. Never send a mutable object as a message.

4. **Do not share actor state.** Never pass `this` or `var` references outside the actor. The actor's state is private to the actor.
