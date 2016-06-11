package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.Point

import scala.util.Random

class GaussianTightSpherePointSource(override val dim: Int,
                                     override val n: Int,
                                     override val k: Int,
                                     override val distance: (Point, Point) => Double)
  extends PointSource {

  override val name = "random-tight-sphere"

  private val zero = Point.zero(dim)

  val sphereSurface = new SphereSurface(dim, 1.0, distance)

  /**
    * An array of points that are far away from each other, including the origin
    */
  override val certificate: Array[Point] =
    sphereSurface.wellSpaced(k, 1024) :+ zero

  override val points: RandomPointIterator = new GaussianTightPointIterator(dim, distance)

}

class GaussianTightPointIterator(val dim:Int,
                                 val distance: (Point, Point) => Double)
  extends RandomPointIterator {

  private val zero = Point.zero(dim)

  override def next(): Point = {
    val p = Point.randomGaussian(dim)
    val radius = math.min(0.01 * Random.nextGaussian(), 1.0)
    p.normalize(distance(p, zero) / radius)
  }


}
