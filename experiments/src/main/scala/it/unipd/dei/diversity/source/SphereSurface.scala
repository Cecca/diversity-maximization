package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.{Distance, Diversity, Point, Utils}

import scala.collection.mutable.ArrayBuffer

/**
  * Utility class that generates points on the surface of
  * a multi dimensional sphere.
  */
class SphereSurface(val dimension: Int,
                    val radius: Double,
                    val distance: (Point, Point) => Double) {

  private val zero = Point.zero(dimension)

  def point(): Point = {
    val p = Point.randomGaussian(dimension)
    p.normalize(distance(p, zero) / radius)
  }

  /**
    * An infinite stream of uniformly distributed points on the
    * surface of the sphere
    */
  def uniformRandom(): Iterable[Point] = new Iterable[Point] {
    override def iterator: Iterator[Point] = new Iterator[Point] {
      override def hasNext: Boolean = true
      override def next(): Point = point()
    }
  }

  def uniformRandom(num: Int): Array[Point] =
    uniformRandom().take(num).toArray

  def wellSpaced(num: Int, tentatives: Int): Array[Point] = {
    val result = ArrayBuffer[Point](point())
    while (result.length < num) {
      val farthest =
        uniformRandom(tentatives).maxBy{ p => minDistance(result, p) }
      result.append(farthest)
    }
    result.toArray
  }

  private def minDistance(points: Seq[Point], point: Point): Double =
    points.view.map(p => distance(p, point)).min

}

object SphereSurface {

  def main(args: Array[String]) {
    val k = 128
    val sg = new SphereSurface(256, 1.0, Distance.euclidean)
    val ur = sg.uniformRandom(k)
    val ws = sg.wellSpaced(k, 1024)
    println(
      s"""
         | Edge diversity
         | ${Diversity.edge(ur, sg.distance)} | ${Diversity.edge(ws, sg.distance)}
         | Clique diversity
         | ${Diversity.clique(ur, sg.distance)} | ${Diversity.clique(ws, sg.distance)}
       """.stripMargin)
  }

}