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

import org.scalacheck._
import org.scalacheck.Prop.{BooleanOperators, forAll, all}

object MapReduceCoresetTest extends Properties("MapReduceCoreset") {

  val distance: (Point, Point) => Double = Distance.euclidean

  def pointGen(dim: Int) =
    for(data <- Gen.listOfN(dim, Gen.choose[Double](0.0, 1.0)))
      yield new Point(data.toArray)

  property("number of points") =
    forAll(Gen.nonEmptyListOf(pointGen(1))) { points =>
      forAll(Gen.choose(2, points.length / 2)) { k =>
        forAll(Gen.choose(k, points.length / 2)) { kernelSize =>
          (points.size > k) ==> {
            val coreset = MapReduceCoreset.run(
              points.toArray, kernelSize, k, distance)

            (coreset.length >= k) :| s"Coreset size is ${coreset.length}"
          }
        }
      }
    }

  property("no duplicates") =
    forAll(Gen.nonEmptyListOf(pointGen(1))) { points =>
      forAll(Gen.choose(2, points.length / 2)) { k =>
        forAll(Gen.choose(k, points.length / 2)) { kernelSize =>
          (points.size > k) ==> {
            val coreset = MapReduceCoreset.run(
              points.toArray, kernelSize, k, distance)

            coreset.points.toSet.size == coreset.length
          }
        }
      }
    }

  val anticoverParameters = for {
    points <- Gen.nonEmptyListOf(pointGen(1))
    k <- Gen.choose(2, points.length)
    kernelSize <- Gen.choose(k, points.length)
  } yield (points, k, kernelSize)

  property("anticover") =
    forAll(Gen.nonEmptyListOf(pointGen(1))) { points =>
      forAll(Gen.choose(2, points.length/2)) { k =>
        forAll(Gen.choose(k, points.length/2)) { kernelSize =>
          val coreset = MapReduceCoreset.run(
            points.toArray, kernelSize, k, distance)

          val radius =
            Utils.maxMinDistance(coreset.delegates, coreset.kernel, distance)

          val farness =
            Utils.minDistance(coreset.kernel, distance)

          (radius <= farness) :| s"radius $radius, farness $farness"
        }
      }
    }

}
