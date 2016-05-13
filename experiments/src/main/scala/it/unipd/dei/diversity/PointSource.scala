package it.unipd.dei.diversity

trait PointSource {

  val distance: (Point, Point) => Double

  val certificate: Array[Point]

  lazy val edgeDiversity: Double = Diversity.edge(certificate, distance)

  lazy val cliqueDiversity: Double = Diversity.clique(certificate, distance)

  lazy val starDiversity: Double = Diversity.star(certificate, distance)

  lazy val treeDiversity: Double = Diversity.tree(certificate, distance)

}
