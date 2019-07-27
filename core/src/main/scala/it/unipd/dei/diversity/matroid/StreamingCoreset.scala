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

package it.unipd.dei.diversity.matroid

import java.util.ConcurrentModificationException

import it.unipd.dei.diversity.Coreset
import com.codahale.metrics.MetricRegistry
import it.unipd.dei.diversity.Utils._

import scala.reflect.ClassTag

object StreamingCoreset {

  def closestPointIndex[T](point: T,
                           points: Array[T],
                           distance: (T, T) => Double,
                           from: Int,
                           until: Int): (Int, Double) = {
    require(from <= until)
    require(until < points.length)
    var minDist = Double.PositiveInfinity
    var minIdx = -1
    var idx = from
    while (idx < until) {
      val dist = distance(point, points(idx))
      if (dist < minDist) {
        minDist = dist
        minIdx = idx
      }
      idx += 1
    }
    (minIdx, minDist)
  }

  /**
    * Swaps the elements at the specified indexes in place
    */
  def swap[T](points: Array[T], i: Int, j: Int): Unit = {
    val tmp = points(i)
    points(i) = points(j)
    points(j) = tmp
  }

}

class StreamingCoreset[T: ClassTag](val kernelSize: Int,
                                    val k: Int,
                                    val matroid: Matroid[T],
                                    val distance: (T, T) => Double)
extends Coreset[T] {

  import StreamingCoreset._

  val metricRegistry = new MetricRegistry()
  val updatesTimer = metricRegistry.timer("StreamingCoreset.update")

  private def farnessInvariant: Boolean =
    (numKernelPoints == 1) || (minKernelDistance >= threshold)

  // When true, accept all the incoming points
  private var _initializing = true

  // Keeps track of the first available position for insertion
  private var _insertionIdx: Int = 0

  private var _threshold: Double = Double.PositiveInfinity

  // The number of times the coreset have been restructured by a merge operation
  private var _numRestructurings: Int = 0

  private val _kernel = Array.ofDim[T](kernelSize + 1)

  private val _incrementalSubsets: Array[IncrementalSubset[T]] = Array.ofDim(_kernel.length)

  private[diversity]
  def initializing: Boolean = _initializing

  private[diversity]
  def threshold: Double = _threshold

  /**
    * The number of times the coreset have been restructured by a merge operation
    */
  private[diversity]
  def numRestructurings: Int = _numRestructurings

  private[diversity]
  def numKernelPoints: Int = _insertionIdx

  /* Only for testing */
  private[diversity]
  def setKernelPoint(index: Int, point: T): Unit = {
    _kernel(index) = point
  }

  private[diversity]
  def addKernelPoint(point: T): Unit = {
    _kernel(_insertionIdx) = point
    _incrementalSubsets(_insertionIdx) = matroid.incrementalSubset(k)
    _insertionIdx += 1
  }

  def pointsIterator: Iterator[T] = kernelPointsIterator ++ _incrementalSubsets.iterator.take(_insertionIdx).flatMap(_.toSeq)

  private[diversity]
  def kernelPointsIterator: Iterator[T] =
    new Iterator[T] {
      val maxIdx = numKernelPoints
      var itIdx = 0

      override def hasNext: Boolean = {
        if (numKernelPoints != maxIdx) {
          throw new ConcurrentModificationException()
        }
        itIdx < maxIdx
      }

      override def next(): T = {
        val elem = _kernel(itIdx)
        itIdx += 1
        elem
      }
    }

  private[diversity]
  def minKernelDistance: Double = minDistance(kernelPointsIterator.toArray[T], distance)

  private def closestKernelDistance(point: T): Double = {
    var m = Double.PositiveInfinity
    kernelPointsIterator.foreach { kp =>
      val d = distance(kp, point)
      if (d < m) {
        m = d
      }
    }
    m
  }

  private[diversity]
  def initializationStep(point: T): Unit = {
    require(_initializing)
    val minDist = closestKernelDistance(point)
    if (minDist < _threshold) {
      _threshold = minDist
    }
    addKernelPoint(point)
    if (_insertionIdx == _kernel.length) {
      _initializing = false
    }
  }

  private def closestKernelPoint(point: T): (Int, Double) = {
    var idx = 0
    var mDist = Double.PositiveInfinity
    var mIdx = 0
    val kps = kernelPointsIterator
    while(kps.hasNext) {
      val d = distance(kps.next(), point)
      if (d < mDist) {
        mDist = d
        mIdx = idx
      }
      idx += 1
    }
    (mIdx, mDist)
  }

  private[diversity]
  def updateStep(point: T): Boolean = {
    require(!_initializing)
    // Find distance to the closest kernel point
    val (minIdx, minDist) = closestKernelPoint(point)
    if (minDist > 2 * _threshold) {
      // Pick the point as a center
      addKernelPoint(point)
      true
    } else {
      _incrementalSubsets(minIdx).add(point)
    }
  }

  private[diversity]
  def swapData(i: Int, j: Int): Unit = {
    swap(_kernel, i, j)
    swap(_incrementalSubsets, i, j)
  }

  private[diversity]
  def resetData(from: Int): Unit = {
    var idx = from
    while (idx < _kernel.length) {
      _incrementalSubsets(idx) = matroid.incrementalSubset(k)
      idx += 1
    }
  }

  private[diversity]
  def merge(): Unit = {
    // Use the `kernel` array as if divided in 3 zones:
    //
    //  - selected: initially empty, stores all the selected nodes.
    //  - candidates: encompasses all the array at the beginning
    //  - discarded: initially empty, stores the points that have been merged
    //
    // The boundaries between these regions are, respectively, the
    // indexes `bottomIdx` and `topIdx`
    require(_insertionIdx == _kernel.length)
    _threshold *= 2
    _numRestructurings += 1

    var bottomIdx = 0
    var topIdx = _kernel.length - 1
    while (bottomIdx < topIdx) {
      val pivot = _kernel(bottomIdx)
      var candidateIdx = bottomIdx + 1
      // Discard the points that are too close to the pivot
      while (candidateIdx <= topIdx) {
        if (distance(pivot, _kernel(candidateIdx)) <= _threshold) {
          // Merge the delegate sets of the pivot and the to-be-discarded candidate
          _incrementalSubsets(bottomIdx) = _incrementalSubsets(bottomIdx).merge(_incrementalSubsets(candidateIdx))
          // Move the candidate (and all its data) to the end of the array
          swapData(candidateIdx, topIdx)
          topIdx -= 1
        } else {
          // Keep the point in the candidate zone
          candidateIdx += 1
        }
      }
      // Move to the next point to be retained
      bottomIdx += 1
    }
    // Reset the data related to excluded points
    resetData(topIdx+1)
    // Set the new insertionIdx
    _insertionIdx = bottomIdx

    // Check the invariant of the minimum distance between kernel points
    assert(farnessInvariant, "Farness after merge")
  }

  /**
    * This method implements the modified doubling algorithm.
    * Return true if the point is added to the inner core-set
    */
  def update(point: T): Boolean = {
    val t = updatesTimer.time()
    // the _insertionIdx variable is modified inside the merge() method
    while (_insertionIdx == _kernel.length) {
      merge()
    }

    val res = if (_initializing) {
      initializationStep(point)
      true
    } else {
      updateStep(point)
    }
    t.stop()
    res
  }

  override def kernel: Vector[T] = kernelPointsIterator.toVector

  override def delegates: Vector[T] = throw new RuntimeException("Operation not supported")

}
