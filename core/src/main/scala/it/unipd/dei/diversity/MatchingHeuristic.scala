package it.unipd.dei.diversity

import scala.collection.mutable
import scala.reflect.ClassTag


object MatchingHeuristic {

  def run[T:ClassTag](points: IndexedSeq[T],
                      k: Int,
                      distance: (T, T) => Double): IndexedSeq[T] = {
    if (points.size <= k) {
      points
    } else {
//      val result = mutable.Set[T]()
      val result = Array.ofDim[T](k)
//      val candidates = mutable.ArrayBuffer[T](points :_*)
      val flags = Array.fill[Boolean](points.length)(true)
      var idx = 0
      while (idx < k/2) {
        // Find the pair of candidates with maximum distance
        var maxDist = 0.0
        var a = 0
        var b = 0
        var i = 0
        while (i<points.length) {
          if (flags(i)) {
            var j = i + 1
            while (j < points.length) {
              if (flags(j)) {
                val d = distance(points(i), points(j))
                if (d > maxDist) {
                  a = i
                  b = j
                  maxDist = d
                }
              }
              j += 1
            }
          }
          i += 1
        }

        // Add the maximum distance pair to the result
        result(2*idx)   = points(a)
        result(2*idx+1) = points(b)
        // Remove the pair from the candidates
        flags(a) = false
        flags(b) = false
//        candidates.remove(candidates.indexOf(a))
//        candidates.remove(candidates.indexOf(b))
        idx += 1
      }
      // If k is odd, add an arbitrary point to the result
      if (k % 2 != 0) {
        var h = 0
        while (!flags(h)) {
          // Find the first true flag
          h += 1
        }
        result(k-1) = points(h)
      }
      result.toArray[T]
    }
  }

}
