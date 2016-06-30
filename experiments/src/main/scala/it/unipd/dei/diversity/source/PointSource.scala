package it.unipd.dei.diversity.source

import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import it.unipd.dei.diversity.{Diversity, Point}

import scala.collection.mutable
import scala.util.Random

trait PointSource extends Iterable[Point] {

  val name: String

  val dim: Int

  val n: Int

  val k: Int

  val distance: (Point, Point) => Double

  val certificate: Array[Point]

  val randomGen: Random

  lazy val edgeDiversity: Double = Diversity.edge(certificate, distance)

  lazy val cliqueDiversity: Double = Diversity.clique(certificate, distance)

  lazy val starDiversity: Double = Diversity.star(certificate, distance)

  lazy val treeDiversity: Double = Diversity.tree(certificate, distance)

  /** The random points that are somehow "close" to each other*/
  def points: RandomPointIterator

  override def iterator: Iterator[Point] =
    new InterleavingPointIterator(certificate, points, n, randomGen)

}

object PointSource {

  def apply(name: String,
            dim: Int,
            n: Int,
            k: Int,
            distance: (Point, Point) => Double,
            randomGen: Random): PointSource = name match {
    case "random-uniform-sphere" =>
      new UniformRandomSpherePointSource(dim, n, k, distance, randomGen)
    case "random-uniform-cube" =>
      new UniformRandomCubePointSource(dim, n, k, distance, randomGen)
    case "random-uniform-sphere-old" =>
      new OldUniformRandomSpherePointSource(dim, n, k, distance, randomGen)
    case "random-gaussian-sphere" =>
      new GaussianRandomSpherePointSource(dim, n, k, distance, randomGen)
    case "random-gaussian-sphere-old" =>
      new OldGaussianRandomSpherePointSource(dim, n, k, distance, randomGen)
    case str =>
      throw new IllegalArgumentException(s"Unknown source $str")
  }

}

class InterleavingPointIterator(val certificate: Array[Point],
                                val points: Iterator[Point],
                                val num: Int,
                                val randomGen: Random)
extends Iterator[Point] {

  private var _cnt = 0
  private val _toEmit = mutable.Set[Point](certificate :_*)
  private val _emissionProb: Double = certificate.length.toDouble / num

  override def hasNext: Boolean = _toEmit.nonEmpty || _cnt < num

  override def next(): Point = {
    _cnt += 1
    if (_toEmit.nonEmpty && randomGen.nextDouble() <= _emissionProb) {
      val p = _toEmit.head
      _toEmit.remove(p)
      p
    } else {
      points.next()
    }
  }
}
