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

import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import it.unipd.dei.diversity.Point

import scala.util.Random

class GaussianRandomSpherePointSource(override val dim: Int,
                                      override val n: Long,
                                      override val k: Int,
                                      override val distance: (Point, Point) => Double,
                                      val randomGen: Random)
  extends PointSource {

  override val name = "gaussian-random-sphere"

  private val zero = Point.zero(dim)

  val sphereSurface = new SphereSurface(dim, 1.0, distance, randomGen)

  /**
    * An array of points that are far away from each other.
    */
  override val certificate: Array[Point] =
    sphereSurface.wellSpaced(k, 1024)
  
  override val points: RandomPointIterator =
    new GaussianRandomPointIterator(dim, distance, randomGen)

}

class GaussianRandomPointIterator(val dim:Int,
                                  val distance: (Point, Point) => Double,
                                  val randomGen: Random)
extends RandomPointIterator {

  private val zero = Point.zero(dim)
  val sphereSurface = new SphereSurface(dim, 1.0, distance, randomGen)
  val surfaceProbability = 0.0

  override def next(): Point = {
    // Generate a random point inside the sphere, or on the surface
    if (randomGen.nextDouble() < surfaceProbability) {
      sphereSurface.point()
    } else {
      val p = Point.randomGaussian(dim, randomGen)
      val dist = distance(p, zero)
      val radius = math.abs(0.25 * Random.nextGaussian())
      val normFactor =
        if (radius >= 1.0) dist
        else               dist/radius
      val res = p.normalize(normFactor)
      require(distance(res, zero) <= 1.00000000001, // Allow a little tolerance
        s"Point must be within radius 1, was ${distance(res,zero)} instead " +
          s"(original distance $dist, random radius $radius, normalization factor $normFactor)")
      res
    }
  }


}
