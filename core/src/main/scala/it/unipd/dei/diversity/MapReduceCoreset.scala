package it.unipd.dei.diversity

import scala.reflect.ClassTag

object MapReduceCoreset {
  
  def run[T:ClassTag](points: Array[T],
                      kernelSize: Int,
                      numDelegates: Int,
                      distance: (T, T) => Double): Array[T] = {
    val resultSize = kernelSize * (numDelegates+1)
    if (points.length < resultSize) {
      points
    } else {
      val kernel = FarthestPointHeuristic.run(points, kernelSize, distance)
      val result = Array.ofDim[T](resultSize)
      var resultIdx = 0
      while (resultIdx < kernel.size) {
        result(resultIdx) = kernel(resultIdx)
        resultIdx += 1
      }

      val counters = Array.fill[Int](kernel.length)(0)

      var pointIdx = 0
      while (pointIdx < points.length) {
        if (!kernel.contains(points(pointIdx))) {
          // Find the closest center
          var centerIdx = 0
          var minDist = Double.PositiveInfinity
          var minIdx = -1
          while (centerIdx < kernel.length) {
            val dist = distance(points(pointIdx), kernel(centerIdx))
            if (dist < minDist) {
              minDist = dist
              minIdx = centerIdx
            }
            centerIdx += 1
          }
          // Add the point to the solution if there is space in the delegate count
          if (counters(minIdx) < numDelegates) {
            assert(minDist < Utils.minDistance(kernel, distance),
              s"Distance: $minDist, radius ${Utils.minDistance(kernel, distance)}")
            result(resultIdx) = points(pointIdx)
            counters(minIdx) += 1
            resultIdx += 1
          }
        }
        pointIdx += 1
      }
      result.take(resultIdx)
    }
  }

}
