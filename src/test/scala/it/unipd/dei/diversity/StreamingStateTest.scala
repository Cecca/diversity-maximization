package it.unipd.dei.diversity

import org.scalatest.{FreeSpec, Matchers}

class StreamingStateTest extends FreeSpec with Matchers {

  "The update" - {
    "should accept the first k'+1 points" in {
      val kPrime = 10
      val points = (0 to kPrime).view.map(_ => Point.random(4))
      val sState = new StreamingState[Point](kPrime, 0, Distance.euclidean)

      sState.isInitializing should be (true)

      points.foreach { p =>
        sState.update(p) should be (true)
      }

      sState.isInitializing should be (false)
    }

    "after initialization, should reject the already seen points" in {
      val kPrime = 10
      val points = (0 to kPrime).view.map(_ => Point.random(4))
      val sState = new StreamingState[Point](kPrime, 0, Distance.euclidean)

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

    "after initialization, the threshold must be the minimum distance" in {
      val kPrime = 10
      val points = (0 to kPrime).view.map(_ => Point.random(4))
      val sState = new StreamingState[Point](kPrime, 0, Distance.euclidean)

      sState.isInitializing should be (true)
      points.foreach { p =>
        sState.update(p) should be (true)
      }
      sState.isInitializing should be (false)

      sState.getThreshold should be (StreamingState.minDistance[Point](
        sState.kernel, Distance.euclidean))
    }
  }

  "The delegate adding" - {
    "Should add the delegate only if thehre is space" in {
      val sState = new StreamingState[Point](2, 1, Distance.euclidean)
      sState.addDelegate(0, Point(0)) should be (true)
      sState.addDelegate(0, Point(1)) should be (false)
      sState.addDelegate(1, Point(3)) should be (true)
    }
  }

  "The delegate merging" - {
    "Should add only the delegates there's space for" in {
      val sState = new StreamingState[Point](2, 3, Distance.euclidean)
      sState.kernel(0) = Point(0)
      sState.addDelegate(0, Point(1)) should be (true)
      sState.addDelegate(0, Point(2)) should be (true)
      sState.kernel(1) = Point(3)
      sState.addDelegate(1, Point(4)) should be (true)
      sState.addDelegate(1, Point(5)) should be (true)
      sState.addDelegate(1, Point(6)) should be (true)

      sState.mergeDelegates(0, 1)
      sState.delegatesOf(0).toList should be (List(Point(1), Point(2), Point(3)))
    }
  }

}
