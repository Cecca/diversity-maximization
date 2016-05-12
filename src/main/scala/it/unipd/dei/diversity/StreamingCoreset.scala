package it.unipd.dei.diversity

import java.util.ConcurrentModificationException

import scala.reflect.ClassTag

object StreamingCoreset {

  def minDistance[T](points: Array[T],
                     distance: (T, T) => Double): Double =
    points.flatMap { p1 =>
      points.flatMap { p2 =>
        if (p1 != p2) {
          Iterable(distance(p1, p2))
        } else {
          Iterable.empty
        }
      }
    }.min

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

class StreamingCoreset[T:ClassTag](val kernelSize: Int,
                                   val numDelegates: Int,
                                   val distance: (T, T) => Double) {
  import StreamingCoreset._

  // When true, accept all the incoming points
  var _initializing = true

  // Keeps track of the first available position for insertion
  var _insertionIdx: Int = 0

  var _threshold: Double = Double.PositiveInfinity

  val _kernel = Array.ofDim[T](kernelSize + 1)

  // Kernel points are not explicitly stored as delegates.
  val _delegates: Array[Array[T]] = Array.ofDim[T](_kernel.length, numDelegates)
  val _delegateCounts = Array.ofDim[Int](_kernel.length)

  def initializing: Boolean = _initializing

  def threshold: Double = _threshold

  def numKernelPoints: Int = _insertionIdx

  def delegatesOf(index: Int): Iterator[T] =
    new Iterator[T] {
      var itIdx = 0
      val maxIdx = _delegateCounts(index)

      override def hasNext: Boolean = itIdx < maxIdx

      override def next(): T = {
        val elem = _delegates(index)(itIdx)
        itIdx += 1
        elem
      }
    }

  def points: Iterator[T] = kernelPoints ++ delegatePoints

  def kernelPoints: Iterator[T] =
    new Iterator[T] {
      val maxIdx = _insertionIdx
      var itIdx = 0

      override def hasNext: Boolean = {
        if (_insertionIdx != maxIdx) {
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

  def delegatePoints: Iterator[T] =
    (0 until _insertionIdx).iterator.flatMap { idx =>
      delegatesOf(idx)
    }

  def minKernelDistance: Double = minDistance(kernelPoints.toArray, distance)

  def initializationStep(point: T): Unit = {
    require(_initializing)
    _kernel(_insertionIdx) = point
    val (_, minDist) = closestKernelPoint(point)
    if (minDist < _threshold) {
      _threshold = minDist
    }
    _insertionIdx += 1
    if (_insertionIdx == _kernel.length) {
      _initializing = false
    }
  }

  def addDelegate(index: Int, point: T): Boolean = {
    if (_delegateCounts(index) < numDelegates) {
      _delegates(index)(_delegateCounts(index)) = point
      _delegateCounts(index) += 1
      true
    } else {
      false
    }
  }

  def updateStep(point: T): Boolean = {
    require(!_initializing)
    // Find distance to the closest kernel point
    val (minIdx, minDist) = closestKernelPoint(point)
    if (minDist > 2*_threshold) {
      // Pick the point as a center
      _kernel(_insertionIdx) = point
      _insertionIdx += 1
      true
    } else {
      // Add as a delegate, if possible
      addDelegate(minIdx, point)
    }
  }

  def swapData(i: Int, j: Int): Unit = {
    swap(_kernel, i, j)
    swap(_delegateCounts, i, j)
    swap(_delegates, i, j)
  }

  def mergeDelegates(center: Int, merged: Int): Unit = {
    // Try to add the merged point as a delegate of the center
    addDelegate(center, _kernel(merged))
    val dels = delegatesOf(merged)
    // Add delegates while we have space
    while(dels.hasNext && addDelegate(center, dels.next())) {}
  }

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

    var bottomIdx = 0
    var topIdx = _kernel.length - 1
    while(bottomIdx < topIdx) {
      val pivot = _kernel(bottomIdx)
      var candidateIdx = bottomIdx+1
      // Discard the points that are too close to the pivot
      while (candidateIdx <= topIdx) {
        if (distance(pivot, _kernel(candidateIdx)) <= _threshold) {
          // Merge the delegate sets of the pivot and the to-be-discarded candidate
          mergeDelegates(bottomIdx, candidateIdx)
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
    // Set the new insertionIdx
    _insertionIdx = bottomIdx

    // Check the invariant of the minimum distance between kernel points
    assert(numKernelPoints == 1 || minKernelDistance >= threshold,
      s"minKernelDist=$minKernelDistance, threshold=$threshold")
  }

  /**
    * This method implements the modified doubling algorithm.
    * Return true if the point is added to the inner core-set
    */
  def update(point: T): Boolean = {
    while (_insertionIdx == _kernel.length) {
      merge()
    }

    if (_initializing) {
      initializationStep(point)
      true
    } else {
      updateStep(point)
    }
  }

  private def closestKernelPoint(point: T): (Int, Double) = {
    StreamingCoreset.closestPointIndex(point, _kernel, distance, 0, _insertionIdx)
  }

}
