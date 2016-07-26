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

import scala.collection.mutable
import scala.reflect.ClassTag

object Diversity {

  /**
    * Finds the remote-edge diversity of the given set of points.
    */
  def edge[T: ClassTag](points: IndexedSeq[T],
                        distance: (T, T) => Double): Double = {
    Utils.minDistance(points, distance)
  }

  /**
    * Find a subset of k points whose minimum distance is
    * a factor 2 away from the optimum
    */
  def edge[T: ClassTag](points: IndexedSeq[T],
                        k: Int,
                        distance: (T, T) => Double): Double = {
    val kSubset = FarthestPointHeuristic.run(points, k, distance)
    edge(kSubset, distance)
  }

  def clique[T: ClassTag](points: IndexedSeq[T],
                          distance: (T, T) => Double): Double =
    Utils.pairs(points).map { case (p1, p2) =>
      distance(p1, p2)
    }.sum


  def clique[T: ClassTag](points: IndexedSeq[T],
                          k: Int,
                          distance: (T, T) => Double): Double = {
    val kSubset = MatchingHeuristic.run(points, k, distance)
    clique(kSubset, distance)
  }

  def tree[T: ClassTag](points: IndexedSeq[T],
                        distance: (T, T) => Double): Double = {
    // Find a minimum spanning tree
    val result = mutable.Set[T](points.head)
    val candidates = mutable.Set[T](points.tail :_*)
    val weights = mutable.ArrayBuffer[Double]()
    while (candidates.nonEmpty) {
      // Find the closest point
      val (a, b, dist) = {
        for {
          p1 <- result
          p2 <- candidates
        } yield (p1, p2, distance(p1, p2))
      }.view.minBy(_._3)
      result.add(a)
      result.add(b)
      candidates.remove(a)
      candidates.remove(b)
      weights.append(dist)
    }
    weights.sum
  }

  def tree[T: ClassTag](points: IndexedSeq[T],
                        k: Int,
                        distance: (T, T) => Double): Double = {
    val kSubset = FarthestPointHeuristic.run(points, k, distance)
    tree(kSubset, distance)
  }

  def star[T: ClassTag](points: IndexedSeq[T],
                        distance: (T, T) => Double): Double = {
    // Try all the points as centers, looking for the
    // one that minimizes the distance to all the others
    points.view.map { center =>
      points.view.map { p => distance(center, p) }.sum
    }.min
  }

  def star[T: ClassTag](points: IndexedSeq[T],
                        k: Int,
                        distance: (T, T) => Double): Double = {
    val kSubset = MatchingHeuristic.run(points, k, distance)
    star(kSubset, distance)
  }

}
