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
      val result = mutable.Set[T]()
      val candidates = mutable.ArrayBuffer[T](points :_*)
      var idx = 0
      while (idx < k/2) {
        // Find the pair of candidates with maximum distance
        var maxDist = 0.0
        var a = candidates.head
        var b = candidates.head
        var i = 0
        while (i<candidates.size) {
          var j = i +1
          while (j < candidates.size) {
            val d = distance(candidates(i), candidates(j))
            if (d > maxDist) {
              a = candidates(i)
              b = candidates(j)
              maxDist = d
            }
            j += 1
          }
          i += 1
        }

        // Add the maximum distance pair to the result
        result.add(a)
        result.add(b)
        // Remove the pair from the candidates
        candidates.remove(candidates.indexOf(a))
        candidates.remove(candidates.indexOf(b))
        idx += 1
      }
      // If k is odd, add an arbitrary point to the result
      if (k % 2 != 0) {
        result.add(candidates.head)
      }
      result.toArray[T]
    }
  }

}
