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
  * Compare the matching heuristic implementation with the time it
  * takes to sum all the pairwise distances for k times.
  */
object MatchingHeuristicBench extends Bench.OfflineReport {

  val randomGen = new Random()

  val distance: (Point, Point) => Double = Distance.euclidean

  val sets: Gen[Array[Point]] = for {
    size <- Gen.range("size")(100, 500, 100)
  } yield Array.ofDim[Point](size).map{_ => Point.random(64, randomGen)}

  val ks: Gen[Int] = Gen.range("k")(10, 90, 10)

  val params: Gen[(Array[Point], Int)] = for {
    points <- sets
    k <- ks
  } yield (points, k)

  performance of "matching" in {
    measure method "baseline" in {
      using(params) in { case (points, k) =>
        var h = 0
        while(h < k) {
          var sum = 0.0
          var i = 0
          while (i < points.length) {
            var j = i + 1
            while (j < points.length) {
              sum += distance(points(i), points(j))
              j += 1
            }
            i += 1
          }
          h += 1
        }
      }
    }

    measure method "sequential" in {
      using(params) in { case (points, k) =>
        MatchingHeuristic.runSeq(points, k, distance)
      }
    }

    measure method "parallel" in {
      using(params) in { case (points, k) =>
        MatchingHeuristic.runPar(points, k, distance)
      }
    }

  }

}
