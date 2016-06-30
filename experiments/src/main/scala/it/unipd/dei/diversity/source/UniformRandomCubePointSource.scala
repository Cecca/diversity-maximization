package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.Point

import scala.util.Random

class UniformRandomCubePointSource (override val dim: Int,
                                    override val n: Int,
                                    override val k: Int,
                                    override val distance: (Point, Point) => Double,
                                    val randomGen: Random)
  extends PointSource {

  override val name = "uniform-random-cube"

  override val points: RandomPointIterator =
    new UniformRandomCubePointIterator(dim, randomGen)

  /**
    * An array of points that are far away from each other.
    */
  override val certificate: Array[Point] =
    points.take(k).toArray

}

class UniformRandomCubePointIterator(val dim:Int,
                                     val randomGen: Random)
  extends RandomPointIterator {


  override def next(): Point = Point.random(dim, randomGen)


}

