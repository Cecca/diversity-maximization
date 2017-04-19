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

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap
import it.unipd.dei.diversity.matroid.Matroid

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
                             distance: (T, T) => Double): Array[T] = {
    if (input.length <= k) {
      input.toArray
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
      val partial = initialSet(input, k, distance)
      require(partial.length == k)
      // The "inside" and "outside" sets as an array of flags
      val flags = Array.fill[Boolean](input.length)(false)
      // set the initial partial solution
      var h = 0
      while(h < input.length) {
        if (partial.contains(input(h))) {
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
      require(partial.length == k)
      partial
    }
  }

  def runMatroid[T:ClassTag](input: Array[T],
                             k: Int,
                             epsilon: Double,
                             matroid: Matroid[T],
                             distance: (T, T) => Double,
                             diversity: (IndexedSubset[T], (T, T) => Double) => Double)
  : IndexedSeq[T] = {
    if (input.length <= k) {
      input
    } else {
      val is = matroid.independentSetOfSize(input, k)
      require(is.size == k, s"No idependent set of size $k in the input of LocalSearch")

      var foundImprovingSwap = true
      while(foundImprovingSwap) {
        // This will be reset to true if a swap is found
        foundImprovingSwap = false
        // Compute the threshold for this iteration
        val threshold = (1+epsilon/k)*diversity(is, distance)
        println(s"Threshold: $threshold")

        // Try to find an improving swap
        var i = 0
        while (i < input.length && !foundImprovingSwap) {
          if (is.contains(i)) { // If i is inside the partial solution
            var j = i + 1
            while (j < input.length && !foundImprovingSwap) {
              if (!is.contains(j)) { // If j is not inside the partial solution
                // Try the swap
                is.remove(i) // move i-th point outside the solution
                is.add(j) // move j-th point inside the solution
                if (matroid.isIndependent(is) && diversity(is, distance) > threshold) {
                  // Swap successful, set foundImprovingSwap to break the inner loops
                  foundImprovingSwap = true
                } else {
                  // Swap unsuccessful, reset to previous situation
                  is.add(i) // move i-th point inside the solution again
                  is.remove(j) // move j-th point outside the solution again
                }
              }
              j += 1
            }
          }
          i += 1
        }
      }
      is.toVector
    }
  }

  private def cliqueDiversity[T](subset: IndexedSubset[T],
                                 distance: (T, T) => Double): Double = {
    val n = subset.superSet.length
    var currentDiversity: Double = 0
    var i = 0
    while (i<n) {
      if (subset.contains(i)) {
        var j = i + 1
        while (j < n) {
          if (subset.contains(j)) {
            currentDiversity += distance(subset.get(i).get, subset.get(j).get)
          }
          j += 1
        }
      }
      i += 1
    }
    currentDiversity
  }

  private def sumDistances[T](from: T,
                              to: IndexedSubset[T],
                              distance: (T, T) => Double): Double = {
    var sum: Double = 0
    val n = to.superSet.size
    var i = 0
    while (i<n) {
      if (to.contains(i)) {
        sum += distance(from, to.superSet(i))
      }
      i += 1
    }
    sum
  }

  // TODO Add metrics, like invocations to the oracle, invocations to the distance function, etc
  def remoteClique[T:ClassTag](input: IndexedSeq[T],
                               k: Int,
                               epsilon: Double,
                               matroid: Matroid[T],
                               distance: (T, T) => Double)
  : IndexedSeq[T] = {
    if (input.length <= k) {
      input
    } else {
      val is = matroid.independentSetOfSize(input, k)
      require(is.size == k, s"No idependent set of size $k in the input of LocalSearch")

      // initialize contribution map
      val contribs = new Int2DoubleOpenHashMap(k)
      var ci = 0
      while (ci < input.size) {
        if (is.contains(ci)) {
          contribs.put(ci, sumDistances(input(ci), is, distance))
        }
        ci += 1
      }
      var currentDiversity = contribs.values().toDoubleArray.sum / 2.0
      println(s"Diversity of the initial solution: $currentDiversity")

      var foundImprovingSwap = true
      while(foundImprovingSwap) {
        // This will be reset to true if a swap is found
        foundImprovingSwap = false
        // Compute the threshold for this iteration
        val threshold = (1+epsilon/k)*currentDiversity
        println(s"Diversity: $currentDiversity")
        assert(Math.abs(contribs.values().toDoubleArray.sum / 2.0 - cliqueDiversity(is, distance)) <= 0.00000001,
          s"Diversities differ! ${contribs.values().toDoubleArray.sum / 2.0} != $currentDiversity\n" +
            s"Difference: ${Math.abs(contribs.values().toDoubleArray.sum / 2.0 - currentDiversity)}\n" +
            s"$contribs")

        // Try to find an improving swap
        var i = 0
        while (i < input.length && !foundImprovingSwap) {
          if (is.contains(i)) { // If i is inside the partial solution
            val insideContribution = contribs.get(i)
            var j = i + 1
            while (j < input.length && !foundImprovingSwap) {
              if (!is.contains(j)) { // If j is not inside the partial solution
                // Try the swap
                is.remove(i) // move i-th point outside the solution
                is.add(j) // move j-th point inside the solution
                if (matroid.isIndependent(is)) {
                  val outsideContribution = sumDistances(input(j), is, distance)
                  val diversityWithSwap = currentDiversity - insideContribution + outsideContribution
                  if (diversityWithSwap > threshold) {
                    // Swap successful, set foundImprovingSwap to break the inner loops
                    foundImprovingSwap = true
                    currentDiversity = diversityWithSwap
                    // Update the contribution map. Should recompute from scratch because of all the new edges.
                    contribs.remove(i)
                    val keys = contribs.keySet().iterator()
                    while (keys.hasNext) {
                      val k = keys.nextInt()
                      contribs.addTo(k, distance(input(k), input(j)) - distance(input(k), input(i)))
                    }
                    contribs.put(j, outsideContribution)
                  }
                }
                if (!foundImprovingSwap) {
                  // Swap unsuccessful, reset to previous situation
                  is.add(i) // move i-th point inside the solution again
                  is.remove(j) // move j-th point outside the solution again
                }
              }
              j += 1
            }
          }
          i += 1
        }
      }
      is.toVector
    }
  }


  def coreset[T:ClassTag](points: IndexedSeq[T],
                          k: Int,
                          epsilon: Double,
                          distance: (T, T) => Double,
                          diversity: (IndexedSeq[T], (T, T) => Double) => Double)
  : MapReduceCoreset[T] =
    new MapReduceCoreset(
      run(points, k, epsilon, distance, diversity).toVector,
      Vector.empty[T])

}
