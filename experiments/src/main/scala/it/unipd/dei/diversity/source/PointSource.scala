package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.{Diversity, Point}

trait PointSource extends Iterator[Point] {

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