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

object Utils {

  /**
    * Generates all the (n choose 2) pairs. The sequence is
    * required to be indexes in order to be efficiently avoid duplicates
    */
  def pairs[T](points: IndexedSeq[T]): Iterator[(T, T)] =
    {
      for {
        i <- points.indices.iterator
        j <- ((i+1) until points.size).iterator
      } yield (points(i), points(j))
    }

  def minDistance[T](points: IndexedSeq[T],
                     distance: (T, T) => Double): Double = {
    require(points.length >= 2, "At least two points are needed")
    pairs(points).map{case (a, b) => distance(a,b)}.min
  }

  def maxMinDistance[T](pointsA: IndexedSeq[T],
                        pointsB: IndexedSeq[T],
                        distance: (T, T) => Double): Double = {
    require(pointsA.nonEmpty && pointsB.nonEmpty, "At least two points are needed")
    pointsA.map { p1 =>
      pointsB.map { p2 =>
        distance(p1, p2)
      }.min
    }.max
  }

}
