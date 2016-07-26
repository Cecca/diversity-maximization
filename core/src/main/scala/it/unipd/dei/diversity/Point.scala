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

package it.unipd.dei.diversity

import java.util

import it.unimi.dsi.util.XorShift1024StarRandomGenerator

import scala.util.Random

class Point(val data: Array[Double]) extends Comparable[Point] with Serializable {

  def dimension: Int = data.length

  def apply(idx: Int): Double = data(idx)

  def normalize(factor: Double): Point =
    Point(data.map{x => x/factor})

  override def equals(other: Any): Boolean =
    other match {
      case that: Point =>
        this.data.sameElements(that.data)
      case _ => false
    }

  override def compareTo(other: Point): Int = {
    require(this.dimension == other.dimension)
    var i: Int = 0
    while (i < data.length) {
      if (this.data(i) < other.data(i)) {
        return -1
      } else if (this.data(i) > other.data(i)) {
        return 1
      }
      i += 1
    }
    0
  }

  override def hashCode(): Int = util.Arrays.hashCode(data)

  override def toString: String =
    data.mkString("(", ", ", ")")

}

object Point {

  def apply(data: Double*): Point = new Point(data.toArray)

  def apply(data: Array[Double]): Point = new Point(data)

  def random(dimension: Int, randomGen: Random): Point =
    Point((0 until dimension).view.map { _ =>
      randomGen.nextDouble()
    }.toArray)

  def randomGaussian(dimension: Int, randomGen: Random): Point =
    Point((0 until dimension).view.map { _ =>
      randomGen.nextGaussian()
    }.toArray)

  def zero(dimension: Int): Point =
    Point(Array.fill[Double](dimension)(0.0))

}