package it.unipd.dei.diversity

import scala.collection.mutable
import scala.util.Random

class RandomSpherePointSource(val dim: Int,
                              val n: Int,
                              val k: Int,
                              override val distance: (Point, Point) => Double)
  extends PointSource with Iterator[Point] {

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
      p.normalize(distance(p, zero) / Random.nextDouble())
    }
  }

}

object RandomSpherePointSource {

  def main(args: Array[String]) {
    val s = new RandomSpherePointSource(100, 1000, 10, Distance.euclidean)
    val points = s.toArray
    println(
      s"""
         |edge:   ${s.edgeDiversity}
         |clique: ${s.cliqueDiversity}
         |tree:   ${s.treeDiversity}
         |star:   ${s.starDiversity}
         |
         |Number of points generated: ${points.length}
         |minimum distance between all points: ${Diversity.edge(points, Distance.euclidean)}
         |
         |Estimated remote-edge:   ${Diversity.edge(points, s.k, Distance.euclidean)}
         |Estimated remote-clique: ${Diversity.clique(points, s.k, Distance.euclidean)}
         |Estimated remote-tree:   ${Diversity.tree(points, s.k, Distance.euclidean)}
         |Estimated remote-star:   ${Diversity.star(points, s.k, Distance.euclidean)}
       """.stripMargin)

    println("======================")
    println(Diversity.clique(points, s.k, Distance.euclidean))
  }

}
