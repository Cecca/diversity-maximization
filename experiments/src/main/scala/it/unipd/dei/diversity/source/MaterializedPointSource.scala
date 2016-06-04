package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.{Diversity, FarthestPointHeuristic, MatchingHeuristic, Point}

class MaterializedPointSource(override val name: String,
                              override val dim: Int,
                              override val k: Int,
                              val allPoints: Vector[Point],
                              override val certificate: Array[Point],
                              override val distance: (Point, Point) => Double)
extends PointSource {

  override val n: Int = allPoints.length

  override def points: RandomPointIterator = throw new UnsupportedOperationException()

  override def iterator: Iterator[Point] = allPoints.iterator

  private lazy val farthestSubset = FarthestPointHeuristic.run(allPoints, k, certificate(0), distance)

  private lazy val matchingSubset = MatchingHeuristic.run(allPoints, k, distance)

  override lazy val edgeDiversity: Double = Diversity.edge(farthestSubset, distance)

  override lazy val cliqueDiversity: Double = Diversity.clique(matchingSubset, distance)

  override lazy val starDiversity: Double = Diversity.star(matchingSubset, distance)

  override lazy val treeDiversity: Double = Diversity.tree(farthestSubset, distance)

}
