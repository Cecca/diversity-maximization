package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.Point

import scala.collection.mutable
import scala.util.Random

class UniformRandomSpherePointSource(override val dim: Int,
                                     override val n: Int,
                                     override val k: Int,
                                     override val distance: (Point, Point) => Double)
  extends PointSource {

  override val name = "sphere-uniform-random"

  private val zero = Point.zero(dim)

  /**
    * An array of points that are far away from each other.
    */
  override val certificate: Array[Point] = {
    (0 until k).map { _ =>
      val p = Point.randomGaussian(dim)
      p.normalize(distance(p, zero))
    }.toArray
  }

  override val points: Iterator[Point] = new Iterator[Point] {
    override def hasNext: Boolean = true

    override def next(): Point = {
      // Generate a random point inside the sphere
      val p = Point.randomGaussian(dim)
      p.normalize(distance(p, zero) / (0.8 * Random.nextDouble()))
    }
  }

}
