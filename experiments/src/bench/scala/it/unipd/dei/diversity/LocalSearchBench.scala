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

object LocalSearchBench extends Bench.OfflineReport {

  val randomGen = new Random()

  val distance: (Point, Point) => Double = Distance.euclidean

  val diversity: (IndexedSeq[Point], (Point, Point) => Double) => Double =
    Diversity.clique[Point]

  val epsilon = 1.0

  val sets: Gen[Array[Point]] = for {
    size <- Gen.range("size")(100, 300, 100)
  } yield Array.ofDim[Point](size).map{_ => Point.random(10, randomGen)}

  val ks: Gen[Int] = Gen.range("k")(10, 60, 10)

  val params: Gen[(Array[Point], Int)] = for {
    points <- sets
    k <- ks
  } yield (points, k)

  performance of "local search" in {

    measure method "local search (slow)" in {
      using(params) in { case (points, k) =>
        LocalSearch.run(points, k, epsilon, distance, diversity)
      }
    }

    measure method "local search (memoized)" in {
      using(params) in { case (points, k) =>
        LocalSearch.runMemoized(points, k, epsilon, distance, LocalSearch.cliqueDiversity)
      }
    }

    measure method "matching heuristic" in {
      using(params) in { case (points, k) =>
        MatchingHeuristic.run(points, k, distance)
      }
    }
  }

}
