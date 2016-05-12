package it.unipd.dei.diversity

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

}
