package it.unipd.dei.diversity

import scala.collection.immutable.HashSet
import scala.reflect.ClassTag

/**
  * The local search algorithm of [AghamolaeiFZ15] and [IndykMMM14]
  *
  * [IndykMMM14]
  * - Indyk, P., Mahabadi, S., Mahdian, M., & Mirrokni,
  *   V. S. (2014). Composable core-sets for diversity and coverage
  *   maximization. In , Proceedings of the 33rd ACM SIGMOD-SIGACT-SIGART
  *   Symposium on Principles of Database Systems (pp. 100–108). New York,
  *   NY, USA: ACM.
  *
  * [AghamolaeiFZ15]
  * - Aghamolaei Sepideh, Majid Farhadi, and Hamid Zarrabi-Zadeh.
  *   "Diversity Maximization via Composable Coresets."
  */
object LocalSearch {

  def initialSet[T:ClassTag](input: IndexedSeq[T],
                             k: Int,
                             distance: (T, T) => Double): IndexedSeq[T] = {
    if (input.length <= k) {
      input
    } else {
      val result = Array.ofDim[T](k)
      // Find the pair of points with maximum distance
      var maxDist = 0.0
      var a = 0
      var b = 0
      var i = 0
      while (i<input.length) {
        var j = i+1
        while (j<input.length) {
          val d = distance(input(i), input(j))
          if (d > maxDist) {
            a = i
            b = j
            maxDist = d
          }
          j += 1
        }
        i += 1
      }
      // Add them to the result
      result(0) = input(a)
      result(1) = input(b)

      // Fill the point set with the other points
      var h = 0
      var rIdx = 2
      while (h<input.length && rIdx < k) {
        if (h != a && h != b) {
          result(rIdx) = input(h)
          rIdx += 1
        }
        h += 1
      }

      result
    }
  }

  // Modify the partial array in place
  def flagsToArray[T:ClassTag](points: IndexedSeq[T],
                               flags: Array[Boolean],
                               partial: Array[T]): Unit = {
    var i = 0
    var j = 0
    while (i<flags.length) {
      if (flags(i)) {
        partial(j) = points(i)
        j += 1
      }
      i += 1
    }
  }

  def run[T:ClassTag](input: IndexedSeq[T],
                      k: Int,
                      epsilon: Double,
                      distance: (T, T) => Double,
                      diversity: (IndexedSeq[T], (T, T) => Double) => Double)
  : IndexedSeq[T] = {
    if (input.length <= k) {
      input
    } else {
      // Partial solution: it will be used to store the partial results
      val partial = Array.ofDim[T](k)
      // The "inside" and "outside" sets as an array of flags
      val flags = Array.ofDim[Boolean](input.length)
      // set the initial partial solution
      // TODO There's room for optimization here, by modifying initialSet
      // to return the bitmap.
      val initial = initialSet(input, k, distance)
      var h = 0
      while(h < input.length) {
        if (initial.contains(input(h))) {
          flags(h) = true
        }
        h += 1
      }
      var foundImprovingSwap = true
      while(foundImprovingSwap) {
        // This will be reset to true if a swap is found
        foundImprovingSwap = false
        // Compute the threshold for this iteration
        flagsToArray(input, flags, partial)
        val threshold = (1+epsilon/k)*diversity(partial, distance)

        // Try to find an improving swap
        var i = 0
        while (i < flags.length && !foundImprovingSwap) {
          if (flags(i)) { // If i is inside the partial solution
            var j = i + 1
            while (j < flags.length && !foundImprovingSwap) {
              if (!flags(j)) { // If j is not inside the partial solution
                // Try the swap
                flags(i) = false // move i-th point outside the solution
                flags(j) = true  // move j-th point inside the solution
                flagsToArray(input, flags, partial)
                if (diversity(partial, distance) > threshold) {
                  // Swap successful, set foundImprovingSwap to break the inner loops
                  foundImprovingSwap = true
                } else {
                  // Swap unsuccessful, reset to previous situation
                  flags(i) = true  // move i-th point inside the solution again
                  flags(j) = false // move j-th point outside the solution again
                }
              }
              j += 1
            }
          }
          i += 1
        }
      }

      flagsToArray(input, flags, partial)
      partial
    }
  }

}
