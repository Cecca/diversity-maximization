package it.unipd.dei.diversity

object StreamingState {

  def closestPointIndex(point: Point,
                        points: Array[Point],
                        distance: (Point, Point) => Double,
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

class StreamingState(val kernelSize: Int,
                     val numDelegates: Int,
                     val distance: (Point, Point) => Double) {
  import StreamingState.swap

  // When true, accept all the incoming points
  private var _initializing = true

  // Keeps track of the first available position for insertion
  private var insertionIdx: Int = 0

  private var threshold: Double = Double.PositiveInfinity

  val kernel = Array.ofDim[Point](kernelSize + 1)

  // Kernel points are not explicitly stored as delegates.
  val delegates: Array[Array[Point]] = Array.ofDim[Point](kernel.length, numDelegates)
  val delegateCounts = Array.ofDim[Int](kernel.length)

  def isInitializing: Boolean = _initializing

  def initializationStep(point: Point): Unit = {
    require(_initializing)
    kernel(insertionIdx) = point
    val (_, minDist) = closestKernelPoint(point)
    if (minDist < threshold) {
      threshold = minDist
    }
    insertionIdx += 1
    if (insertionIdx == kernel.length) {
      _initializing = false
    }
  }

  def updateStep(point: Point): Boolean = {
    require(!_initializing)
    // Find distance to the closest kernel point
    val (minIdx, minDist) = closestKernelPoint(point)
    if (minDist > 2*threshold) {
      // Pick the point as a center
      kernel(insertionIdx) = point
      insertionIdx += 1
      true
    } else if (delegateCounts(minIdx) < numDelegates) {
      // Add the point as a delegate
      delegates(minIdx)(delegateCounts(minIdx)) = point
      delegateCounts(minIdx) += 1
      true
    } else {
      // Just ignore the point
      false
    }
  }

  def swapData(i: Int, j: Int): Unit = {
    swap(kernel, i, j)
    swap(delegateCounts, i, j)
    swap(delegates, i, j)
  }

  def mergeDelegates(center: Int, merged: Int): Unit = {
    var maxDelegatesToAdd =
      math.min(numDelegates - delegateCounts(center), delegateCounts(merged))
    if (maxDelegatesToAdd > 0) {
      delegates(center)(delegateCounts(center)) = kernel(merged)
      delegateCounts(center) += 1
      maxDelegatesToAdd -= 1
      var idx = 0
      while (idx < maxDelegatesToAdd) {
        delegates(center)(delegateCounts(center)) = delegates(merged)(idx)
        idx += 1
        delegateCounts(center) += 1
      }
    }
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
    require(insertionIdx == kernel.length)
    threshold *= 2

    var bottomIdx = 0
    var topIdx = kernel.length - 1
    while(bottomIdx < topIdx) {
      val pivot = kernel(bottomIdx)
      var candidateIdx = bottomIdx+1
      // Discard the points that are too close to the pivot
      while (candidateIdx <= topIdx) {
        if (distance(pivot, kernel(candidateIdx)) <= threshold) {
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
    insertionIdx = bottomIdx + 1
  }

  /**
    * This method implements the modified doubling algorithm.
    * Return true if the point is added to the inner core-set
    */
  def update(point: Point): Boolean = {
    while (insertionIdx == kernel.length) {
      merge()
    }

    if (_initializing) {
      initializationStep(point)
      true
    } else {
      updateStep(point)
    }
  }

  private def closestKernelPoint(point: Point): (Int, Double) = {
    StreamingState.closestPointIndex(point, kernel, distance, 0, insertionIdx)
  }

  def coreset(): Array[Point] = {
    val result = Array.ofDim[Point](kernel.length*numDelegates)
    var idx = 0
    kernel.foreach { p =>
      result(idx) = p
      idx += 1
    }
    delegates.zip(delegateCounts).foreach { case (dl, count) =>
      var cnt = count - 1
      while (cnt >= 0) {
        result(idx) = dl(cnt)
        cnt -= 1
        idx += 1
      }
    }
    result
  }

}
