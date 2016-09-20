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

import scala.reflect.ClassTag


object MatchingHeuristic {

  def run[T:ClassTag](points: IndexedSeq[T],
                      k: Int,
                      distance: (T, T) => Double): IndexedSeq[T] =
    runPar(points, k, distance)

  def runSeq[T:ClassTag](points: IndexedSeq[T],
                      k: Int,
                      distance: (T, T) => Double): IndexedSeq[T] = {
    if (points.size <= k) {
      points
    } else {
      val result = Array.ofDim[T](k)
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

  def runPar[T:ClassTag](points: IndexedSeq[T],
                         k: Int,
                         distance: (T, T) => Double): IndexedSeq[T] = {
    if (points.size <= k) {
      points
    } else {
      val result = Array.ofDim[T](k)
      val flags = Array.fill[Boolean](points.length)(true)
      var idx = 0
      while (idx < k/2) {
        // Find the pair of candidates with maximum distance
        // var maxDist = 0.0
        // var a = 0
        // var b = 0
        var i = 0
        val (maxDist, a, b) = (0 until points.length).par.map { i =>
          var _maxDist = 0.0
          var _a = 0
          var _b = 0
          if (flags(i)) {
            var j = i + 1
            while (j < points.length) {
              if (flags(j)) {
                val d = distance(points(i), points(j))
                if (d > _maxDist) {
                  _a = i
                  _b = j
                  _maxDist = d
                }
              }
              j += 1
            }
          }
          (_maxDist, _a, _b)
        }.maxBy(_._1)

        // Add the maximum distance pair to the result
        result(2*idx)   = points(a)
        result(2*idx+1) = points(b)
        // Remove the pair from the candidates
        flags(a) = false
        flags(b) = false
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
