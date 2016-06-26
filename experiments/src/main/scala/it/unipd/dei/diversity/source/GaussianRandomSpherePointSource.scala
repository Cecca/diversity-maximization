package it.unipd.dei.diversity.source

import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import it.unipd.dei.diversity.Point

import scala.util.Random

class GaussianRandomSpherePointSource(override val dim: Int,
                                      override val n: Int,
                                      override val k: Int,
                                      override val distance: (Point, Point) => Double)
  extends PointSource {

  override val name = "gaussian-random-sphere"

  private val zero = Point.zero(dim)

  val sphereSurface = new SphereSurface(dim, 1.0, distance)

  /**
    * An array of points that are far away from each other.
    */
  override val certificate: Array[Point] =
    sphereSurface.wellSpaced(k, 1024)
  
  override val points: RandomPointIterator = new GaussianRandomPointIterator(dim, distance)

}

class GaussianRandomPointIterator(val dim:Int,
                                  val distance: (Point, Point) => Double)
extends RandomPointIterator {

  private val zero = Point.zero(dim)
  val sphereSurface = new SphereSurface(dim, 1.0, distance)
  val surfaceProbability = 0.25
  val randomGen = new XorShift1024StarRandomGenerator()

  override def next(): Point = {
    // Generate a random point inside the sphere, or on the surface
    if (randomGen.nextDouble() < surfaceProbability) {
      sphereSurface.point()
    } else {
      val p = Point.randomGaussian(dim)
      val radius = math.min(0.25 * Random.nextGaussian(), 1.0)
      p.normalize(distance(p, zero) / radius)
    }
  }


}
