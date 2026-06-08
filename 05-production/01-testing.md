# Testing ``

Testing data pipelines is different from testing web applications. You test transformations (pure functions), integrations (Spark, databases), and actor behavior (message protocols).

## Testing Pure Functions

Pure functions are the easiest to test. Given input, assert output:

```scala
// In src/test/scala/EventTransformerTest.scala
// Using ScalaTest

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EventTransformerTest extends AnyFunSuite with Matchers:

  val transformer = EventTransformer

  test("parseValidEvent returns CleanEvent for valid input"):
    val raw = RawEvent("e1", """{"type":"click","amount":10.5}""")
    val result = transformer.parse(raw)
    result shouldBe Right(CleanEvent("e1", "click", 10.5))

  test("parseValidEvent returns Left for invalid JSON"):
    val raw = RawEvent("e2", "not-json")
    val result = transformer.parse(raw)
    result.isLeft shouldBe true

  test("filterByAmount keeps events above threshold"):
    val events = List(
      CleanEvent("e1", "purchase", 50.0),
      CleanEvent("e2", "purchase", 5.0),
      CleanEvent("e3", "purchase", 150.0)
    )
    val result = transformer.filterHighValue(events, threshold = 100.0)
    result shouldBe List(CleanEvent("e3", "purchase", 150.0))
```

Step by step:

1. Extend `AnyFunSuite` for a flat test structure.
2. Use `test("description")` to define each test case.
3. Use `shouldBe` for assertions.
4. Test both the happy path and error cases.

## Testing Spark Jobs

Use Spark's local mode for testing DataFrame transformations:

```scala
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class SparkJobTest extends AnyFunSuite with Matchers with BeforeAndAfterAll:

  var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

  override def afterAll(): Unit =
    spark.stop()

  test("aggregateByEventType counts events correctly"):
    import spark.implicits.*

    val input = Seq(
      ("e1", "click", 10.0),
      ("e2", "purchase", 50.0),
      ("e3", "click", 0.0)
    ).toDF("id", "eventType", "amount")

    val result = SparkJob.aggregateByEventType(input)
    result.count() shouldBe 2  // two distinct event types

  test("filter removes null event types"):
    import spark.implicits.*

    val input = Seq(
      ("e1", "click", 10.0),
      ("e2", null, 50.0),
      ("e3", "purchase", 0.0)
    ).toDF("id", "eventType", "amount")

    val result = SparkJob.filterValid(input)
    result.count() shouldBe 2
```

Key points:

- Use `master("local[*]")` to run Spark in the test JVM. No cluster needed.
- Disable the UI with `spark.ui.enabled = false` to avoid port conflicts.
- Create small DataFrames from `Seq` for test data.
- Use `BeforeAndAfterAll` to create and stop the SparkSession once for all tests.

## Testing Actors with TestProbe

Akka provides `TestProbe` for testing actor interactions:

```scala
import org.apache.pekko.actor.*
import org.apache.pekko.testkit.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration.*

class EventProcessorTest extends AnyFunSuite with Matchers with BeforeAndAfterAll:

  var system: ActorSystem = _
  var probe: TestProbe = _

  override def beforeAll(): Unit =
    system = ActorSystem("test")
    probe = TestProbe()(system)

  override def afterAll(): Unit =
    system.terminate()

  test("processor increments count on valid event"):
    val processor = system.actorOf(Props[EventProcessor]())
    processor ! ProcessEvent("e1", "valid-data")
    processor ! GetStatus

    // Expect a Status message within 3 seconds
    probe.expectMsgType[Status](3.seconds)
```

`TestProbe` is a test actor that can send messages and assert on received messages. Use it to verify that your actor responds correctly without starting a full actor system.

## Testing Strategy for Data Pipelines

| Layer | What to Test | How |
|-------|-------------|-----|
| Transformations | Pure functions (parse, validate, enrich) | Unit tests with ScalaTest |
| DataFrame operations | Spark SQL logic, aggregations | Local Spark session tests |
| Actor behavior | Message protocols, state changes | TestProbe |
| Integration | End-to-end: read -> transform -> write | Test containers with real services |
| Data quality | Schema, nulls, ranges, uniqueness | Property-based tests or assertions |

Test transformations first -- they are pure functions and cheap to test. Test Spark operations with local mode. Test actors with TestProbe. Integration tests with real services are valuable but expensive; run them in CI, not on every save.
