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

import com.codahale.metrics.MetricRegistry
import it.unipd.dei.diversity.performanceMetrics

object Distance {

  def euclidean(a: Point, b: Point): Double = {
    performanceMetrics.counter("distance-computation")
    require(a.dimension == b.dimension)
    var sum: Double = 0.0
    var i = 0
    while (i<a.dimension) {
      val diff = a(i) - b(i)
      sum += diff*diff
      i += 1
    }
    val res = math.sqrt(sum)
    assert(res < Double.PositiveInfinity, "The distance cannot be infinite! Check your inputs.")
    res
  }

  def euclidean[T](bagA: BagOfWords[T], bagB: BagOfWords[T]): Double = {
    performanceMetrics.counter("distance-computation")
    (bagA, bagB) match {
      case (a: ArrayBagOfWords, b: ArrayBagOfWords) =>
        ArrayBagOfWords.euclidean(a, b)
      case (a, b) =>
        val keys = a.wordUnion(b)
        var sum: Double = 0.0
        for (k <- keys) {
          val diff = a(k) - b(k)
          sum += diff*diff
        }
        val res = math.sqrt(sum)
        assert(res < Double.PositiveInfinity, "The distance cannot be infinite! Check your inputs.")
        res
    }
  }
  
  def jaccard[T](a: BagOfWords[T], b: BagOfWords[T]): Double = {
    performanceMetrics.counter("distance-computation")
    val numerator: Double = a.wordUnion(b).map { w =>
      math.min(a(w), b(w))
    }.sum
    val denominator: Double = a.wordUnion(b).map { w =>
      math.max(a(w), b(w))
    }.sum
    1.0 - (numerator/denominator)
  }

  def cosineSimilarity[T](a: BagOfWords[T], b: BagOfWords[T]): Double = {
    require(a.words.nonEmpty)
    require(b.words.nonEmpty)
    val keys = a.wordIntersection(b)
    var numerator: Double = 0.0
    for(k <- keys) {
      numerator += a(k) * b(k)
    }
    val denomA = math.sqrt(a.words.map(w => a(w)*a(w)).sum)
    val denomB = math.sqrt(b.words.map(w => b(w)*b(w)).sum)
    val res = numerator / (denomA * denomB)
    // The minimum taken below fixes a bug of the cosineDistance function. Because
    // of floating point arithmetic, sometimes the result of the above computations
    // can be slightly greater than 1.0. However, it makes no sense to compute
    // math.acos of a value greater than, therefore the implementation returns NaN
    // in that case instead of 0.0
    //
    // Actually, the lack of precision of the Double type is a real hassle when
    // computing trigonometric functions.
    math.min(res, 1.0)
  }

  def cosineDistance[T](a: BagOfWords[T], b: BagOfWords[T]): Double = {
    PerformanceMetrics.distanceFnCounterInc()
    2*math.acos(cosineSimilarity(a,b)) / math.Pi
  }

}
