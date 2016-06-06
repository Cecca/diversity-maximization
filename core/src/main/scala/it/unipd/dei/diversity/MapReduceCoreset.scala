package it.unipd.dei.diversity

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class MapReduceCoreset[T:ClassTag](val centers: Vector[T],
                                   val delegates: Vector[T]) extends Serializable {

  def points: Vector[T] = centers ++ delegates

  def length: Int = centers.length + delegates.length

}

object MapReduceCoreset {

  def compose[T:ClassTag](a: MapReduceCoreset[T], b: MapReduceCoreset[T]): MapReduceCoreset[T] =
    new MapReduceCoreset(
      (a.centers ++ b.centers).distinct,
      (a.delegates ++ b.delegates).distinct)
  
  def run[T:ClassTag](points: Array[T],
                      kernelSize: Int,
                      numDelegates: Int,
                      distance: (T, T) => Double): MapReduceCoreset[T] = {
    val resultSize = kernelSize * (numDelegates+1)
    if (points.length < resultSize) {
      new MapReduceCoreset(points.take(kernelSize).toVector, points.drop(kernelSize).toVector)
    } else {
      val kernel = FarthestPointHeuristic.run(points, kernelSize, distance)
      val delegates = ArrayBuffer[T]()
//      val result = Array.ofDim[T](resultSize)
//      var resultIdx = 0
//      while (resultIdx < kernel.size) {
//        result(resultIdx) = kernel(resultIdx)
//        resultIdx += 1
//      }

      val counters = Array.fill[Int](kernel.length)(0)

      var pointIdx = 0
      while (pointIdx < points.length) {
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
        // Add the point to the solution if there is space in the delegate count.
        // Consider only distances greater than zero in order not to add the
        // centers again.
        if (minDist > 0.0 && counters(minIdx) < numDelegates) {
          assert(minDist < Utils.minDistance(kernel, distance),
            s"Distance: $minDist, radius ${Utils.minDistance(kernel, distance)}")
          delegates.append(points(pointIdx))
//          result(resultIdx) = points(pointIdx)
          counters(minIdx) += 1
//          resultIdx += 1
        }
        pointIdx += 1
      }
//      result.take(resultIdx)
      new MapReduceCoreset(kernel.toVector, delegates.toVector)
    }
  }

}
