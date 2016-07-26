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

import org.scalameter.api._
import scala.util.Random

/**
  * Benchmark StreamingCoreset: the baseline is summing
  * the euclidean norms of all the points in the stream.
  */
object StreamingCoresetBench extends Bench.OfflineReport {

  val randomGen = new Random()

  val spaceDimension = 128

  val distance: (Point, Point) => Double = Distance.euclidean

  val sets: Gen[Array[Point]] = for {
    size <- Gen.range("num-points")(10000, 30000, 10000)
  } yield Array.ofDim[Point](size).map{_ => Point.random(spaceDimension, randomGen)}

  val ks: Gen[Int] = Gen.range("k")(10, 30, 10)

  val kernelSizes: Gen[Int] = Gen.range("k")(100, 300, 100)

  val params: Gen[(Array[Point], Int, Int)] = for {
    points <- sets
    size <- kernelSizes
    k <- ks
  } yield (points, size, k)

  performance of "streaming" in {

    measure method "baseline" in {
      using(params) in { case (points, size, k) =>
        val zero = Point(Array.fill[Double](spaceDimension)(0.0))
        var sum = 0.0
        var i = 0
        while (i < points.length) {
          sum += distance(points(i), zero)
          i += 1
        }
      }
    }

    measure method "coreset" in {
      using(params) in { case (points, size, k) =>
        val coreset = new StreamingCoreset[Point](size, k, distance)
        var i = 0
        while (i < points.length) {
          coreset.update(points(i))
          i += 1
        }
      }
    }


  }


}
