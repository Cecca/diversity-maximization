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

class UniformRandomSpherePointSource(override val dim: Int,
                                     override val n: Long,
                                     override val k: Int,
                                     override val distance: (Point, Point) => Double,
                                     val randomGen: Random)
  extends PointSource {

  override val name = "uniform-random-sphere"

  private val zero = Point.zero(dim)

  /**
    * An array of points that are far away from each other.
    */
  override val certificate: Array[Point] =
    new SphereSurface(dim, 1.0, distance, randomGen).wellSpaced(k, 1024)

  override val points: RandomPointIterator = new UniformRandomPointIterator(dim, distance, randomGen)
}

class UniformRandomPointIterator(val dim:Int,
                                 val distance: (Point, Point) => Double,
                                 val randomGen: Random)
extends RandomPointIterator {

  private val zero = Point.zero(dim)

  override def next(): Point = {
    // Generate a random point inside the sphere
    val p = Point.randomGaussian(dim, randomGen)
    val res = p.normalize(distance(p, zero) / randomGen.nextDouble())
    require(distance(res, zero) <= 1.00000000001) // Allow a little tolerance
    res
  }


}
