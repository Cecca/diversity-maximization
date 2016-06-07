package it.unipd.dei.diversity

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class MapReduceCoreset[T:ClassTag](val kernel: Vector[T],
                                   val delegates: Vector[T])
extends Coreset[T] with Serializable {

  def length: Int = kernel.length + delegates.length

  override def toString: String =
    s"Coreset with ${kernel.size} centers and ${delegates.size} delegates"

}

object MapReduceCoreset {

  def compose[T:ClassTag](a: MapReduceCoreset[T], b: MapReduceCoreset[T]): MapReduceCoreset[T] =
    new MapReduceCoreset(
      (a.kernel ++ b.kernel).distinct,
      (a.delegates ++ b.delegates).distinct)
  
  def run[T:ClassTag](points: Array[T],
                      kernelSize: Int,
                      numDelegates: Int,
                      distance: (T, T) => Double): MapReduceCoreset[T] = {
    val resultSize = kernelSize * (numDelegates+1)
    if (points.length < kernelSize) {
      new MapReduceCoreset(points.toVector, Vector.empty[T])
    } else {
      val kernel = FarthestPointHeuristic.run(points, kernelSize, distance)
      val delegates = ArrayBuffer[T]()

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
        assert(minDist <= Utils.minDistance(kernel, distance),
          s"Distance: $minDist, farness: ${Utils.minDistance(kernel, distance)}")
        // Add the point to the solution if there is space in the delegate count.
        // Consider only distances greater than zero in order not to add the
        // centers again.
        if (minDist > 0.0 && counters(minIdx) < numDelegates) {
          delegates.append(points(pointIdx))
          counters(minIdx) += 1
        }
        pointIdx += 1
      }
      assert(Utils.maxMinDistance(delegates, kernel, distance) <= Utils.minDistance(kernel, distance),
        "Anticover property failing")
      new MapReduceCoreset(kernel.toVector, delegates.toVector)
    }
  }

}
