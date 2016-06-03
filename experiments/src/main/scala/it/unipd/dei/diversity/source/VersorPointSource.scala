package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.{Distance, Diversity, Point}

class VersorPointSource (override val dim: Int,
                         override val n: Int,
                         override val k: Int,
                         override val distance: (Point, Point) => Double)
  extends PointSource {

  require(k <= 2*dim,
    s"Can generate only 2*dim certificate points (k=$k, dim=$dim)")

  override val name = "sphere-gaussian-random"

  override val certificate: Array[Point] = {
    val cts = Array.fill[Point](k)(Point.zero(dim))
    var i = 0
    while (i < cts.length) {
      val coord = i / 2
      val verse =
        if (i % 2 == 0) 1.0
        else               -1.0
      cts(i).data(coord) = verse
      i += 1
    }
    cts
  }

  override val points: RandomPointIterator = new GaussianRandomPointIterator(dim, distance)

}

object VersorPointSource {
  def main(args: Array[String]) {
    val k = 15
    val dim = 256
    val n = 1024
    val distance: (Point, Point) => Double = Distance.euclidean

    val vps = new VersorPointSource(dim, n, k, distance)
    val gps = new GaussianRandomSpherePointSource(dim, n, k, distance)

    println(
      s"""
        | Versor:
        | ${Diversity.edge(vps.certificate, distance)} | ${Diversity.clique(vps.certificate, distance)}
        |
        | Random:
        | ${Diversity.edge(gps.certificate, distance)} | ${Diversity.clique(gps.certificate, distance)}
      """.stripMargin)
  }
}