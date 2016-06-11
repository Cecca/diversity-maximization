package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.Point

class GaussianPointSource (override val dim: Int,
                               override val n: Int,
                               override val k: Int,
                               override val distance: (Point, Point) => Double)
  extends PointSource {

  override val name = "gaussian"

  private val zero = Point.zero(dim)

  val sphereSurface = new SphereSurface(dim, 1.0, distance)

  /**
    * An array of points that are far away from each other.
    */
  override val certificate: Array[Point] =
    sphereSurface.wellSpaced(k, 1024)

  override val points: RandomPointIterator = new GaussianPointIterator(dim)
}

class GaussianPointIterator(val dim:Int)
  extends RandomPointIterator {

  override def next(): Point = Point.randomGaussian(dim)

}
