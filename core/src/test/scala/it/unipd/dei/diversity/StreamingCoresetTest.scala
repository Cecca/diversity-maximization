package it.unipd.dei.diversity

import org.scalatest.{FreeSpec, Matchers}
import scala.util.Random

class StreamingCoresetTest extends FreeSpec with Matchers {

  val randomGen = new Random()

  "The update" - {
    "should accept the first k'+1 points" in {
      val kPrime = 10
      val points = (0 to kPrime).view.map(_ => Point.random(4, randomGen))
      val coreset = new StreamingCoreset[Point](kPrime, 0, Distance.euclidean)

      coreset.initializing should be (true)

      points.foreach { p =>
        coreset.update(p) should be (true)
      }

      coreset.initializing should be (false)
    }

    "after initialization, the threshold must be the minimum distance" in {
      val kPrime = 10
      val points = (0 to kPrime).view.map(_ => Point.random(4, randomGen))
      val coreset = new StreamingCoreset[Point](kPrime, 0, Distance.euclidean)

      coreset.initializing should be (true)
      points.foreach { p =>
        coreset.update(p) should be (true)
      }
      coreset.initializing should be (false)

      coreset.threshold should be (coreset.minKernelDistance)
    }
  }

  "The delegate adding" - {
    "Should add the delegate only if thehre is space" in {
      val coreset = new StreamingCoreset[Point](2, 1, Distance.euclidean)
      coreset.addDelegate(0, Point(0)) should be (true)
      coreset.addDelegate(0, Point(1)) should be (false)
      coreset.addDelegate(1, Point(3)) should be (true)
    }
  }

  "The delegate merging" - {
    "Should add only the delegates there's space for" in {
      val coreset = new StreamingCoreset[Point](2, 3, Distance.euclidean)
      coreset.setKernelPoint(0, Point(0))
      coreset.addDelegate(0, Point(1)) should be (true)
      coreset.addDelegate(0, Point(2)) should be (true)
      coreset.setKernelPoint(1, Point(3))
      coreset.addDelegate(1, Point(4)) should be (true)
      coreset.addDelegate(1, Point(5)) should be (true)
      coreset.addDelegate(1, Point(6)) should be (true)

      coreset.mergeDelegates(0, 1)
      coreset.delegatesOf(0).toList should be (List(Point(1), Point(2), Point(3)))
    }
  }

}
