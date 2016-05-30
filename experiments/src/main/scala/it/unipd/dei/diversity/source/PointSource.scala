package it.unipd.dei.diversity.source

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

  lazy val edgeDiversity: Double = Diversity.edge(certificate, distance)

  lazy val cliqueDiversity: Double = Diversity.clique(certificate, distance)

  lazy val starDiversity: Double = Diversity.star(certificate, distance)

  lazy val treeDiversity: Double = Diversity.tree(certificate, distance)

  /** The random points that are somehow "close" to each other*/
  val points: RandomPointIterator

  override def iterator: Iterator[Point] = new Iterator[Point] {

    private val _toEmit = mutable.Set[Point](certificate :_*)
    private val _emissionProb: Double = k.toDouble / n

    override def hasNext: Boolean = _toEmit.nonEmpty

    override def next(): Point =
      if (Random.nextDouble() <= _emissionProb) {
        val p = _toEmit.head
        _toEmit.remove(p)
        p
      } else {
        points.next()
      }
  }

}

object PointSource {

  def apply(name: String,
            dim: Int,
            n: Int,
            k: Int,
            distance: (Point, Point) => Double): PointSource = name match {
    case "random-uniform-sphere" =>
      new UniformRandomSpherePointSource(dim, n, k, distance)
    case "random-gaussian-sphere" =>
      new GaussianRandomSpherePointSource(dim, n, k, distance)
    case str =>
      throw new IllegalArgumentException(s"Unknown source $str")
  }

}