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

  def run[T:ClassTag](input: IndexedSeq[T],
                      k: Int,
                      epsilon: Double,
                      distance: (T, T) => Double,
                      diversity: (IndexedSeq[T], (T, T) => Double) => Double)
  : IndexedSeq[T] = {
    if (input.length <= k) {
      input
    } else {
      var partial: Set[T] = initialSet(input, k, distance).toSet
      var outside: Set[T] =
        input.iterator.filterNot(p => partial.contains(p)).toSet

      // While there are convenient swappings
      var foundPairs = true
      while (foundPairs) {
        val threshold = (1+epsilon/k)*diversity(partial.toArray[T], distance)
        // try (lazily) all the combinations to find an improving swap
        val swaps: Iterator[(Set[T], Set[T])] =
          partial.iterator.flatMap { in =>
            outside.iterator.flatMap { out =>
              val swapin = partial - in + out
              val swapout = outside - out + in
              val div = diversity(swapin.toArray[T], distance)
              if (div > threshold) {
                Iterator((swapin, swapout))
              } else {
                Iterator.empty
              }
            }
          }
        if (swaps.hasNext) {
          val (swapin, swapout) = swaps.next()
          partial = swapin
          outside = swapout
        } else {
          // There are no improving swaps, break from the loop
          foundPairs = false
        }
      }

      partial.toArray[T]
    }
  }

}
