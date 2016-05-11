package it.unipd.dei.diversity

object StreamingState {



}

class StreamingState(val kernelSize: Int,
                     val numDelegates: Int,
                     val distance: (Point, Point) => Double) {

  private var _spaceDimension: Int = -1

  // When true, accept all the incoming points
  private var _initializing = true

  // Keeps track of the first available position for insertion
  private var insertionIdx: Int = 0

  private var threshold: Double = Double.PositiveInfinity

  val kernel = Array.ofDim[Point](kernelSize + 1)

  // Kernel points are not explicitly stored as delegates.
  val delegates: Array[Array[Point]] = Array.ofDim[Point](kernel.length, numDelegates)
  val delegateCounts = Array.ofDim[Int](kernel.length)

  private def checkDimension(point: Point): Boolean =
    if (_spaceDimension < 0) {
      _spaceDimension = point.dimension
      true
    } else {
      _spaceDimension == point.dimension
    }

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

  def updateStep(point: Point): Unit = {
    require(!_initializing)
    // Find distance to the closest kernel point
    val (minIdx, minDist) = closestKernelPoint(point)
    if (minDist > 2*threshold) {
      // Pick the point as a center
      kernel(insertionIdx) = point
      insertionIdx += 1
    } else if (delegateCounts(minIdx) < numDelegates) {
      // Add the point as a delegate
      delegates(minIdx)(delegateCounts(minIdx)) = point
      delegateCounts(minIdx) += 1
    } else {
      // Just ignore the point
    }
  }

  def merge(): Unit = {

  }

  /**
    * This method implements the modified doubling algorithm
    */
  def update(point: Point): Unit = {
    require(checkDimension(point))

    while (insertionIdx == kernel.length) {
      merge()
    }

    if (_initializing) {
      initializationStep(point)
    } else {
      updateStep(point)
    }
  }

  private def closestKernelPoint(point: Point): (Int, Double) = {
    var minDist = Double.PositiveInfinity
    var minIdx = -1
    var idx = 0
    while (idx < insertionIdx) {
      val dist = distance(point, kernel(idx))
      if (dist < minDist) {
        minDist = dist
        minIdx = idx
      }
      idx += 1
    }
    (minIdx, minDist)
  }

  def coreset(): Array[Point] = {
    val result = Array.ofDim[Point](kernel.length*numDelegates)
    var idx = 0
    kernel.foreach { p =>
      result(idx) = p
      idx += 1
    }
    delegates.zip(delegateCounts).foreach { case (dl, cnt) =>
      cnt -= 1
      while (cnt >= 0) {
        result(idx) = dl(cnt)
        cnt -= 1
        idx += 1
      }
    }
    result
  }

}
