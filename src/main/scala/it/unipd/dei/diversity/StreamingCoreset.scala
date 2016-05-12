package it.unipd.dei.diversity

import java.util.ConcurrentModificationException

import scala.reflect.ClassTag

object StreamingCoreset {

  def minDistance[T](points: Array[T],
                     distance: (T, T) => Double): Double = {
    require(points.length >= 2, "At least two points are needed")
    points.flatMap { p1 =>
      points.flatMap { p2 =>
        if (p1 != p2) {
          Iterable(distance(p1, p2))
        } else {
          Iterable.empty
        }
      }
    }.min
  }

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
                                    val numDelegates: Int,
                                    val distance: (T, T) => Double) {

  import StreamingCoreset._

  private def farnessInvariant: Boolean =
    (numKernelPoints == 1) || (minKernelDistance >= threshold)

  private def radiusInvariant: Boolean = delegatesRadius <= 2*threshold

  // When true, accept all the incoming points
  private var _initializing = true

  // Keeps track of the first available position for insertion
  private var _insertionIdx: Int = 0

  private var _threshold: Double = Double.PositiveInfinity

  private val _kernel = Array.ofDim[T](kernelSize + 1)

  // Kernel points are not explicitly stored as delegates.
  private val _delegates: Array[Array[T]] = Array.ofDim[T](_kernel.length, numDelegates)
  private val _delegateCounts = Array.ofDim[Int](_kernel.length)

  private[diversity]
  def initializing: Boolean = _initializing

  private[diversity]
  def threshold: Double = _threshold

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
    _insertionIdx += 1
  }

  private[diversity]
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

  private[diversity]
  def kernelPoints: Iterator[T] =
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
  def delegatePoints: Iterator[T] =
    (0 until numKernelPoints).iterator.flatMap { idx =>
      delegatesOf(idx)
    }

  private[diversity]
  def minKernelDistance: Double = minDistance(kernelPoints.toArray, distance)

  /**
    * Find the maximum minimum distance between delegates and kernel points
    */
  private[diversity]
  def delegatesRadius: Double = {
    var radius = 0.0
    delegatePoints.foreach { dp =>
      var curRadius = 0.0
      kernelPoints.foreach { kp =>
        val d = distance(kp, dp)
        if (d < curRadius) {
          curRadius = d
        }
      }
      if (curRadius > radius) {
        radius = curRadius
      }
    }
    radius
  }

  private def closestKernelDistance(point: T): Double = {
    var m = Double.PositiveInfinity
    kernelPoints.foreach { kp =>
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

  private[diversity]
  def addDelegate(index: Int, point: T): Boolean = {
    if (_delegateCounts(index) < numDelegates) {
      _delegates(index)(_delegateCounts(index)) = point
      _delegateCounts(index) += 1
      true
    } else {
      false
    }
  }

  private def closestKernelPoint(point: T): (Int, Double) = {
    var idx = 0
    var mDist = Double.PositiveInfinity
    var mIdx = 0
    val kps = kernelPoints
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
      // Add as a delegate, if possible
      addDelegate(minIdx, point)
    }
  }

  private[diversity]
  def swapData(i: Int, j: Int): Unit = {
    swap(_kernel, i, j)
    swap(_delegateCounts, i, j)
    swap(_delegates, i, j)
  }

  private[diversity]
  def mergeDelegates(center: Int, merged: Int): Unit = {
    // Try to add the merged point as a delegate of the center
    addDelegate(center, _kernel(merged))
    val dels = delegatesOf(merged)
    // Add delegates while we have space
    while (dels.hasNext && addDelegate(center, dels.next())) {}
  }

  private[diversity]
  def resetData(from: Int): Unit = {
    var idx = from
    while (idx < _kernel.length) {
      _delegateCounts(idx) = 0
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

    var bottomIdx = 0
    var topIdx = _kernel.length - 1
    while (bottomIdx < topIdx) {
      val pivot = _kernel(bottomIdx)
      var candidateIdx = bottomIdx + 1
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
    // Reset the data related to excluded points
    resetData(topIdx+1)
    // Set the new insertionIdx
    _insertionIdx = bottomIdx

    // Check the invariant of the minimum distance between kernel points
    assert(farnessInvariant, "Farness after merge")
    // Check the invariant of radius
    assert(radiusInvariant, "Radius after merge")
  }

  /**
    * This method implements the modified doubling algorithm.
    * Return true if the point is added to the inner core-set
    */
  def update(point: T): Boolean = {
    // the _insertionIdx variable is modified inside the merge() method
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

}
