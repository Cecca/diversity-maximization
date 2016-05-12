package it.unipd.dei.diversity

import org.scalatest.{FreeSpec, Matchers}

class StreamingStateTest extends FreeSpec with Matchers {

  "The update" - {
    "should accept the first k'+1 points" in {
      val kPrime = 10
      val points = (0 to kPrime).view.map(_ => Point.random(4))
      val sState = new StreamingState(kPrime, 0, Distance.euclidean)

      sState.isInitializing should be (true)

      points.foreach { p =>
        sState.update(p) should be (true)
      }

      sState.isInitializing should be (false)
    }

    "after initialization, should reject the already seen points" in {
      val kPrime = 10
      val points = (0 to kPrime).view.map(_ => Point.random(4))
      val sState = new StreamingState(kPrime, 0, Distance.euclidean)

      sState.isInitializing should be (true)
      points.foreach { p =>
        sState.update(p) should be (true)
      }
      sState.isInitializing should be (false)
      points.zipWithIndex.foreach { case (p, idx) =>
        withClue(s"Stream index $idx:") {
          sState.update(p) should be (false)
        }
      }
    }
  }

}
