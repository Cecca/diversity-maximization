package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.Point

import scala.collection.mutable
import scala.util.Random

class GaussianRandomSpherePointSource(override val dim: Int,
                                      override val n: Int,
                                      override val k: Int,
                                      override val distance: (Point, Point) => Double)
  extends PointSource {

  override val name = "sphere-gaussian-random"

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

  private val _toEmit = mutable.Set[Point](certificate :_*)

  private val _emissionProb: Double = k.toDouble / n

  override def hasNext: Boolean = _toEmit.nonEmpty

  override def next(): Point = {
    if (Random.nextDouble() <= _emissionProb) {
      val p = _toEmit.head
      _toEmit.remove(p)
      p
    } else {
      // Generate a random point inside the sphere
      val p = Point.randomGaussian(dim)
      val radius = math.min(0.25 * Random.nextGaussian(), 1.0)
      p.normalize(distance(p, zero) / radius)
    }
  }

}
