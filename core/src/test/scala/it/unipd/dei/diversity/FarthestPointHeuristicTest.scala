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
import org.scalacheck.Prop.{forAll, BooleanOperators, all}
import Utils._

object FarthestPointHeuristicTest extends Properties("FarthestPointHeuristic") {

  val distance: (Point, Point) => Double = Distance.euclidean

  property("anticover") =
    forAll(Gen.listOf[Double](Gen.choose[Double](0.0, 1.0)), Gen.choose[Int](2, 100))
    { (pts: List[Double], k: Int) =>
      (pts.length >= 2 && k < pts.size) ==> {
        val points = pts.map(p => Point(p)).toArray
        val result = FarthestPointHeuristic.run(points, k, distance)
        val farness = minDistance(result, distance)
        val radius = maxMinDistance(result, points, distance)
        radius <= farness
      }
    }

  property("simpler implementation") =
    forAll(Gen.listOf[Double](Gen.choose[Double](0.0, 1.0)), Gen.choose[Int](2, 100))
    { (pts: List[Double], k: Int) =>
      (pts.size >= 2 && k < pts.size) ==> {
        val points = pts.map(p => Point(p)).toArray
        val actual = FarthestPointHeuristic.run(points, k, 0, distance).toSet
        val expected = FarthestPointHeuristic.runIdiomatic(points, k, distance).toSet
        s"$actual != $expected" |:(actual == expected)
      }
    }

}
