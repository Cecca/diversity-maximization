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

import it.unipd.dei.diversity.Point

import scala.util.Random

class OldGaussianRandomSpherePointSource (override val dim: Int,
                                          override val n: Int,
                                          override val k: Int,
                                          override val distance: (Point, Point) => Double,
                                          val randomGen: Random)
  extends PointSource {

  override val name = "gaussian-random-sphere-old"

  private val zero = Point.zero(dim)

  val sphereSurface = new SphereSurface(dim, 1.0, distance, randomGen)

  /**
    * An array of points that are far away from each other.
    */
  override val certificate: Array[Point] =
    sphereSurface.wellSpaced(k, 1024)

  override val points: RandomPointIterator =
    new OldGaussianRandomPointIterator(dim, distance, randomGen)

}

class OldGaussianRandomPointIterator(val dim:Int,
                                     val distance: (Point, Point) => Double,
                                     val randomGen: Random)
  extends RandomPointIterator {

  private val zero = Point.zero(dim)
  val sphereSurface = new SphereSurface(dim, 1.0, distance, randomGen)

  override def next(): Point = {
    // This is a old, bugged way of generating the points
    val p = Point.randomGaussian(dim, randomGen)
    val radius = math.min(0.25 * Random.nextGaussian(), 1.0)
    p.normalize(distance(p, zero) / radius)
  }


}
