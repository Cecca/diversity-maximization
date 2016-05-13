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
      val candidates = mutable.Set[T](points :_*)
      while (result.size < k/2) {
        // Find the pair of candidates with maximum distance
        val (a, b, _) = {
          for {
            p1 <- candidates
            p2 <- candidates
          } yield (p1, p2, distance(p1, p2))
        }.view.maxBy(_._3)
        // Add the maximum distance pair to the result
        result.add(a)
        result.add(b)
        // Remove the pair from the candidates
        candidates.remove(a)
        candidates.remove(b)
      }
      // If k is odd, add an arbitrary point to the result
      if (k % 2 != 0) {
        result.add(candidates.head)
      }
      result.toArray[T]
    }
  }

}