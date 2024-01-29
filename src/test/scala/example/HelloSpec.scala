package example

import zio.test._
import zio._


object HelloWorldSpec extends ZIOSpecDefault {
  def spec =
    suite("HelloWorldSpec")(
      test("sayHello") {
        val h = "hello"
        assertTrue(h == "hello")
      },

      test("smokeRun") {
        val app= SimpleApp.run.provideEnvironment(
          ZEnvironment(ZIOAppArgs(Chunk.empty))
        )

        assertZIO(app.unit)(Assertion.isUnit)
      }
    )
}