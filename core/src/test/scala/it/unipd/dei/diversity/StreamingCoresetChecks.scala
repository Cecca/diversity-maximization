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

import org.scalacheck.Prop.{BooleanOperators, forAll}
import org.scalacheck._
import scala.util.Random

object StreamingCoresetChecks extends Properties("StreamingCoreset") {

  val randomGen = new Random()

  val distance: (Point, Point) => Double = Distance.euclidean

  property("farness") =
    forAll(Gen.choose(2, 100), Gen.posNum[Int], Gen.choose(2, 1000))
    { (kernelSize, numDelegates, n) =>
      val coreset = new StreamingCoreset(kernelSize, numDelegates, distance)
      (0 until n).foreach { _ =>
        coreset.update(Point.random(1, randomGen))
      }

      coreset.numKernelPoints == 1 || coreset.threshold <= coreset.minKernelDistance
    }

  property("radius") =
    forAll(Gen.choose(2, 100), Gen.choose(1, 100), Gen.choose(2, 1000))
    { (kernelSize, numDelegates, n) =>
      (n > kernelSize) ==> {
        val coreset = new StreamingCoreset(kernelSize, numDelegates, distance)
        (0 until n).foreach { _ =>
          coreset.update(Point.random(1, randomGen))
        }

        (coreset.delegatesRadius <= 2 * coreset.threshold) :|
          s"Radius: ${coreset.delegatesRadius} > 2*threshold: ${coreset.threshold}"
      }
    }


}
