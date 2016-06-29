package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.Point

import scala.util.Random

class OldUniformRandomSpherePointSource(override val dim: Int,
                                        override val n: Int,
                                        override val k: Int,
                                        override val distance: (Point, Point) => Double,
                                        val randomGen: Random)
  extends PointSource {

  override val name = "uniform-random-sphere"

  private val zero = Point.zero(dim)

  /**
    * An array of points that are far away from each other.
    */
  override val certificate: Array[Point] =
    new SphereSurface(dim, 1.0, distance, randomGen).wellSpaced(k, 1024)

  override val points: RandomPointIterator = new OldUniformRandomPointIterator(dim, distance, randomGen)
}

class OldUniformRandomPointIterator(val dim:Int,
                                    val distance: (Point, Point) => Double,
                                    val randomGen: Random)
  extends RandomPointIterator {

  private val zero = Point.zero(dim)

  override def next(): Point = {
    // Generate a random point inside the sphere
    val p = Point.randomGaussian(dim, randomGen)
    val res = p.normalize(distance(p, zero) / (0.8 * randomGen.nextDouble()))
    require(distance(res, zero) <= 1.00000000001) // Allow a little tolerance
    res
  }


}
