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

import it.unipd.dei.diversity.matroid.Matroid

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class MapReduceCoreset[T:ClassTag](val kernel: Vector[T],
                                   val delegates: Vector[T],
                                   val sizes: Vector[Int],
                                   val radius: Double)
extends Coreset[T] with Serializable {

  def length: Int = kernel.length + delegates.length

  override def toString: String =
    s"Coreset with ${kernel.size} centers and ${delegates.size} delegates"

}

object MapReduceCoreset {

  def composeDistinct[T:ClassTag](a: MapReduceCoreset[T], b: MapReduceCoreset[T]): MapReduceCoreset[T] =
    new MapReduceCoreset(
      (a.kernel ++ b.kernel).distinct,
      (a.delegates ++ b.delegates).distinct,
      (a.sizes ++ b.sizes),
      math.max(a.radius, b.radius))

  def compose[T:ClassTag](a: MapReduceCoreset[T], b: MapReduceCoreset[T]): MapReduceCoreset[T] =
    new MapReduceCoreset(
      a.kernel ++ b.kernel,
      a.delegates ++ b.delegates,
      (a.sizes ++ b.sizes),
      math.max(a.radius, b.radius))


  def run[T:ClassTag](points: Array[T],
                      kernelSize: Int,
                      k: Int,
                      matroid: Matroid[T],
                      distance: (T, T) => Double): MapReduceCoreset[T] = {
    if (points.length < kernelSize) {
      new MapReduceCoreset(points.toVector, Vector.empty[T], points.map(_ => 1).toVector, 0.0)
    } else {
      val start = System.currentTimeMillis
      val startClustering = System.currentTimeMillis
      // FIXME Optimize
      val kernel = FarthestPointHeuristic.run(points, kernelSize, distance)
      var r = 0.0
      val clusters: Map[T, Seq[T]] = points.map { p =>
        val c = kernel.minBy(x => distance(x, p))
        r = math.max(r, distance(c, p))
        (c, p)
      }.groupBy(_._1).mapValues(_.map(_._2))

      require(clusters.size == kernel.size, "The number of clusters should be equal to the number of kernel points")

      val endClustering = System.currentTimeMillis
      println(s"Found clustering of ${clusters.size} in ${endClustering - startClustering}, building coreset")
      val startCoreset = System.currentTimeMillis
      val coresetPoints = clusters.values.par.map { cluster =>
        val dels = matroid.coreSetPoints(cluster, k)
        println(s"Extracted ${dels.size} from cluster of size ${cluster.size}")
        dels
      }

      val sizes = coresetPoints.map(_.size)
      val coreset = coresetPoints.flatten
      val endCoreset = System.currentTimeMillis
      println(s"Built coreset in ${endCoreset - startCoreset}")

      val end = System.currentTimeMillis
      println(s"Coreset built in ${end - start}")

      new MapReduceCoreset[T](coreset.toVector, Vector.empty, sizes.toVector, r)
    }
  }

  def withRadius[T:ClassTag](points: Array[T],
                             radius: Double,
                             k: Int,
                             matroid: Matroid[T],
                             distance: (T, T) => Double): MapReduceCoreset[T] = {
      val kernel = FarthestPointHeuristic.withRadius(points, radius, distance)
      val clusters: Map[T, Seq[T]] = points.map { p =>
        val c = kernel.minBy(x => distance(x, p))
        (c, p)
      }.groupBy(_._1).mapValues(_.map(_._2))

      val coreset = clusters.values.map { cluster =>
        val dels = matroid.coreSetPoints(cluster, k)
        println(s"Extracted ${dels.size} from cluster of size ${cluster.size}")
        dels
      }

      new MapReduceCoreset[T](coreset.flatten.toVector, Vector.empty, coreset.map(_.size).toVector, radius)
  }


  def run[T:ClassTag](points: Array[T],
                      kernelSize: Int,
                      numDelegates: Int,
                      distance: (T, T) => Double): MapReduceCoreset[T] = {
    val resultSize = kernelSize * numDelegates
    if (points.length < kernelSize) {
      new MapReduceCoreset(points.toVector, Vector.empty[T], points.map(_ => 1).toVector, 0.0)
    } else {
      val kernel = FarthestPointHeuristic.run(points, kernelSize, distance)
      val delegates = ArrayBuffer[T]()

      // Init to 1 the number of delegates because we already count the centers
      val counters = Array.fill[Int](kernel.length)(1)
      var radius = 0.0

      var pointIdx = 0
      while (pointIdx < points.length) {
        // Find the closest center
        var centerIdx = 0
        var minDist = Double.PositiveInfinity
        var minIdx = -1
        while (centerIdx < kernel.length) {
          val dist = distance(points(pointIdx), kernel(centerIdx))
          if (dist < minDist) {
            minDist = dist
            minIdx = centerIdx
          }
          centerIdx += 1
        }
        radius = math.max(radius, minDist)
        assert(minDist <= Utils.minDistance(kernel, distance),
          s"Distance: $minDist, farness: ${Utils.minDistance(kernel, distance)}")
        // Add the point to the solution if there is space in the delegate count.
        // Consider only distances greater than zero in order not to add the
        // centers again.
        if (minDist > 0.0 && counters(minIdx) < numDelegates) {
          delegates.append(points(pointIdx))
          counters(minIdx) += 1
        }
        pointIdx += 1
      }
      assert(Utils.maxMinDistance(delegates, kernel, distance) <= Utils.minDistance(kernel, distance),
        "Anticover property failing")
      new MapReduceCoreset(kernel.toVector, delegates.toVector, Vector.empty, radius)
    }
  }

}
