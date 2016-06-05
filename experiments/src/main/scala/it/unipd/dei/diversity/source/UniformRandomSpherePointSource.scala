package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.Point

import scala.collection.mutable
import scala.util.Random

class UniformRandomSpherePointSource(override val dim: Int,
                                     override val n: Int,
                                     override val k: Int,
                                     override val distance: (Point, Point) => Double)
  extends PointSource {

  override val name = "uniform-random-sphere"

  private val zero = Point.zero(dim)

  /**
    * An array of points that are far away from each other.
    */
  override val certificate: Array[Point] =
    new SphereSurface(dim, 1.0, distance).wellSpaced(k, 1024)

  override val points: RandomPointIterator = new UniformRandomPointIterator(dim, distance)
}

class UniformRandomPointIterator(val dim:Int,
                                 val distance: (Point, Point) => Double)
extends RandomPointIterator {

  private val zero = Point.zero(dim)

  override def next(): Point = {
    // Generate a random point inside the sphere
    val p = Point.randomGaussian(dim)
    p.normalize(distance(p, zero) / (0.8 * Random.nextDouble()))
  }


}
