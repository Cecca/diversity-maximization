// diversity-maximization: Diversity maximization in Streaming and MapReduce
// Copyright (C) 2016  Matteo Ceccarello <ceccarel@dei.unipd.it>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
