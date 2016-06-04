package it.unipd.dei.diversity

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object FarthestPointHeuristic {

  def run[T: ClassTag](points: IndexedSeq[T],
                       k: Int,
                       distance: (T, T) => Double): IndexedSeq[T] =
    run(points, k, 0, distance)


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
  def runSlow[T: ClassTag](points: IndexedSeq[T],
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

}
