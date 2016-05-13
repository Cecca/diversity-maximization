package it.unipd.dei.diversity

import scala.reflect.ClassTag

object Diversity {

  /**
    * Finds the remote-edge diversity of the given set of points.
    */
  def edge[T: ClassTag](points: Array[T],
                        distance: (T, T) => Double): Double = {
    Utils.minDistance(points, distance)
  }

  /**
    * Find a subset of k points whose minimum distance is
    * a factor 2 away from the optimum
    */
  def edge[T: ClassTag](points: Array[T],
                        k: Int,
                        distance: (T, T) => Double): Double = {
    val kSubset = FarthestPointHeuristic.run(points, k, distance)
    edge(kSubset, distance)
  }

  def clique[T: ClassTag](points: Array[T],
                          distance: (T, T) => Double): Double = {
    points.flatMap { p1 =>
      points.map { p2 =>
        distance(p1, p2)
      }
    }.sum
  }

  def clique[T: ClassTag](points: Array[T],
                          k: Int,
                          distance: (T, T) => Double): Double = ???

}
