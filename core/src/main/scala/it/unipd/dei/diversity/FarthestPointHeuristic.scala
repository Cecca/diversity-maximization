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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

object FarthestPointHeuristic {

  def run[T: ClassTag](points: IndexedSeq[T],
                       k: Int,
                       distance: (T, T) => Double): IndexedSeq[T] =
    run(points, k, Random.nextInt(points.length), distance)


  def run[T: ClassTag](points: IndexedSeq[T],
                       k: Int,
                       start: Point,
                       distance: (T, T) => Double): IndexedSeq[T] = {
    val idx = points.indexOf(start)
    require(idx > 0, "The starting point should be in the collection!")
    run(points, k, idx, distance)
  }

  def run[T: ClassTag](points: IndexedSeq[T],
                       k: Int,
                       startIdx: Int,
                       distance: (T, T) => Double): IndexedSeq[T] = {
    if (points.length <= k) {
      points
    } else {
      val minDist = Array.fill(points.size)(Double.PositiveInfinity)
      val result = Array.ofDim[T](k)
      // Init the result with an arbitrary point
      result(0) = points(startIdx)
      var i = 1
      while (i < k) {
        var farthest = points(0)
        var maxDist = 0.0

        var h = 0
        while (h < points.length) {
          // Look for the farthest node
          val lastDist = distance(points(h), result(i-1))
          if (lastDist < minDist(h)) {
            minDist(h) = lastDist
          }
          if (minDist(h) > maxDist) {
            maxDist = minDist(h)
            farthest = points(h)
          }
          h += 1
        }
        result(i) = farthest
        i += 1
      }
      result
    }
  }

  def withRadius[T: ClassTag](points: IndexedSeq[T],
                              radius: Double,
                              distance: (T, T) => Double): IndexedSeq[T] =
    withRadius(points, radius, Random.nextInt(points.length), distance)


  def withRadius[T: ClassTag](points: IndexedSeq[T],
                              radius: Double,
                              startIdx: Int,
                              distance: (T, T) => Double): IndexedSeq[T] = {
    val n = points.size
    val minDist = Array.fill(n)(Double.PositiveInfinity)
    val centers = IndexedSubset.apply(points)
    // Init the result with an arbitrary point
    centers.add(startIdx)
    var i = 0
    var radius: Double = 0d
    var nextCenter = 0
    while (i < n) {
      val d = distance(points(startIdx), points(i))
      minDist(i) = d
      if (d > radius) {
        radius = d
        nextCenter = i
      }
      i += 1
    }

    println(s"Radius: $radius (target: $radius)")
    while (radius > radius && centers.size != n) {
      val center = nextCenter
      centers.add(center)
      radius = 0.0
      i = 0
      // Re-compute the radius and find the farthest node
      while (i < n) {
        val d = distance(points(center), points(i))
        if (d < minDist(i)) {
          minDist(i) = d
        }
        if (minDist(i) > radius) {
          radius = minDist(i)
          nextCenter = i
        }
        i += 1
      }
      println(s"Radius: $radius, nextCenter $nextCenter, ${centers.size}/$n (epsilon: $radius)")
    }
    centers.toVector
  }

  /* Just for benchmarking purposes: this is a more idiomatic,
   * albeit slower, implementation of the algorithm
   */
  private[diversity]
  def runSlow[T: ClassTag](points: IndexedSeq[T],
                           k: Int,
                           startIdx: Int,
                           distance: (T, T) => Double): IndexedSeq[T] = {
    if (points.length <= k) {
      points
    } else {
      val result = Array.ofDim[T](k)
      // Init the result with an arbitrary point
      result(0) = points(startIdx)
      var i = 1
      while (i < k) {
        var farthest = points(0)
        var dist = 0.0

        var h = 0
        while (h < points.length) {
          var minDist = Double.PositiveInfinity
          var j = 0
          while (j<i) { 
            val d = distance(result(j), points(h))
            if (d < minDist){
              minDist = d
            }
            j += 1
          }
          if (minDist > dist) {
            dist = minDist
            farthest = points(h)
          }
          h += 1
        }
        result(i) = farthest
        i += 1
      }
      result
    }
  }

  /* Just for testing purposes: this is a more idiomatic,
   * albeit slower, implementation of the algorithm
   */
  private[diversity]
  def runIdiomatic[T: ClassTag](points: IndexedSeq[T],
                                k:Int,
                                distance: (T, T) => Double): IndexedSeq[T] = {
    if (points.length <= k) {
      points
    } else {
      // Initialize with the first point of the input
      val result = ArrayBuffer[T](points(0))
      while(result.size < k) {
        // For each point look for the closest center
        val (farthest, dist) = points.map { p =>
          val dist = result.map { kp =>
            distance(p, kp)
          }.min
          (p, dist)
        }.maxBy(_._2)
        result.append(farthest)
      }
      result.toArray[T]
    }
  }

  def main(args: Array[String]): Unit = {
    val n = args(0).toInt
    val dim = args(1).toInt
    val k = args(2).toInt

    val rnd = new Random()
    val distance: (Point, Point) => Double = Distance.euclidean

    println("Generating points")
    var start = System.currentTimeMillis()
    val points = Array.fill(n)(Point.random(dim, rnd))
    var end = System.currentTimeMillis()
    println(s"Elapsed time ${end - start} ms")

    println("Computing farthest point heuristic")
    start = System.currentTimeMillis()
    run(points, k, distance)
    end = System.currentTimeMillis()
    println(s"Elapsed time ${end - start} ms")
  }

}
