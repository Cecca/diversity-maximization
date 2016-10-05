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
import it.unipd.dei.diversity.{Diversity, Point}

import scala.collection.mutable
import scala.util.Random

trait PointSource extends Iterable[Point] {

  val name: String

  val dim: Int

  val n: Long

  val k: Int

  val distance: (Point, Point) => Double

  val certificate: Array[Point]

  val randomGen: Random

  lazy val edgeDiversity: Double = Diversity.edge(certificate, distance)

  lazy val cliqueDiversity: Double = Diversity.clique(certificate, distance)

  lazy val starDiversity: Double = Diversity.star(certificate, distance)

  lazy val treeDiversity: Double = Diversity.tree(certificate, distance)

  /** The random points that are somehow "close" to each other*/
  def points: RandomPointIterator

  override def iterator: Iterator[Point] =
    new InterleavingPointIterator(certificate, points, n, randomGen)

}

object PointSource {

  def apply(name: String,
            dim: Int,
            n: Long,
            k: Int,
            distance: (Point, Point) => Double,
            randomGen: Random): PointSource = name match {
    case "random-uniform-sphere" =>
      new UniformRandomSpherePointSource(dim, n, k, distance, randomGen)
    case "random-uniform-cube" =>
      new UniformRandomCubePointSource(dim, n, k, distance, randomGen)
    case "chasm-random-uniform-sphere" =>
      new ChasmUniformRandomSpherePointSource(dim, n, k, distance, randomGen)
    case "random-gaussian-sphere" =>
      new GaussianRandomSpherePointSource(dim, n, k, distance, randomGen)
    case "old-random-gaussian-sphere" =>
      new OldGaussianRandomSpherePointSource(dim, n, k, distance, randomGen)
    case str =>
      throw new IllegalArgumentException(s"Unknown source $str")
  }

}

class InterleavingPointIterator(val certificate: Array[Point],
                                val points: Iterator[Point],
                                val num: Long,
                                val randomGen: Random)
extends Iterator[Point] {

  private var _cnt: Long = 0L
  private val _toEmit = mutable.Set[Point](certificate :_*)
  private val _emissionProb: Double = certificate.length.toDouble / num

  override def hasNext: Boolean = _toEmit.nonEmpty || _cnt < num

  override def next(): Point = {
    _cnt += 1
    if (_toEmit.nonEmpty && randomGen.nextDouble() <= _emissionProb) {
      val p = _toEmit.head
      _toEmit.remove(p)
      p
    } else {
      points.next()
    }
  }
}
