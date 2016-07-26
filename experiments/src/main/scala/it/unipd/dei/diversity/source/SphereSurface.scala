// diversity-maximization: Diversity maximization in Streaming and MapReduce
// Copyright (C) 2016  Matteo Ceccarello <ceccarel@dei.unipd.it>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.{Distance, Diversity, Point}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Utility class that generates points on the surface of
  * a multi dimensional sphere.
  */
class SphereSurface(val dimension: Int,
                    val radius: Double,
                    val distance: (Point, Point) => Double,
                    val randomGen: Random) extends Serializable {

  private val zero = Point.zero(dimension)

  def point(): Point = {
    val p = Point.randomGaussian(dimension, randomGen)
    val res = p.normalize(distance(p, zero) / radius)
    require(distance(res, zero) <= 1.00000000001) // Allow a little tolerance
    res
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
