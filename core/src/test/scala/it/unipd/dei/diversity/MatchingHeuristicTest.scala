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

import org.scalacheck.Prop.{forAll, BooleanOperators}
import org.scalacheck.{Gen, Properties}

object MatchingHeuristicTest extends Properties("Matching") {

  val distance: (Point, Point) => Double = Distance.euclidean

  property("size") =
    forAll(Gen.listOf[Double](Gen.choose[Double](0.0, 1.0)), Gen.choose[Int](2, 100))
    { (pts: List[Double], k: Int) =>
      (pts.size >= 2 && k < pts.size) ==> {
        val points = pts.map(p => Point(p)).toArray
        val kSubset = MatchingHeuristic.run(points, k, distance).toSet
        (kSubset.size == k) :| s"Actual size ${kSubset.size}"
      }
  }

}
