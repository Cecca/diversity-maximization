package it.unipd.dei.diversity

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object FarthestPointHeuristic {

  def run[T: ClassTag](points: Array[T],
                       k: Int,
                       distance: (T, T) => Double): Array[T] = {
    if (points.length <= k) {
      points
    } else {
      val result = Array.ofDim[T](k)
      // Init the result with an arbitrary point
      result(0) = points(0)
      var i = 1
      while (i < k) {
        var farthest = points(0)
        var dist = 0.0
        points.foreach { p =>
          val closestCtrDist = result.slice(0, i).view.map{q => distance(p, q)}.min
          if (closestCtrDist > dist) {
            dist = closestCtrDist
            farthest = p
          }
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
  def runSlow[T: ClassTag](points: Array[T],
                           k:Int,
                           distance: (T, T) => Double): Array[T] = {
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
      result.toArray
    }
  }

}
