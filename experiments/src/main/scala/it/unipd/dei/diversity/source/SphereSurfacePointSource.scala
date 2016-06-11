package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.Point

class SphereSurfacePointSource(override val dim: Int,
                               override val n: Int,
                               override val k: Int,
                               override val distance: (Point, Point) => Double)
extends PointSource {

  override val name = "random-sphere-surface"

  private val zero = Point.zero(dim)

  val sphereSurface = new SphereSurface(dim, 1.0, distance)

  /**
    * An array of points that are far away from each other.
    */
  override val certificate: Array[Point] =
    sphereSurface.wellSpaced(k, 1024)

  override val points: RandomPointIterator = new SphereSurfacePointIterator(dim, distance)
}

class SphereSurfacePointIterator(val dim:Int,
                                 val distance: (Point, Point) => Double)
  extends RandomPointIterator {

  val sphereSurface = new SphereSurface(dim, 1.0, distance)

  override def next(): Point = sphereSurface.point()

}


