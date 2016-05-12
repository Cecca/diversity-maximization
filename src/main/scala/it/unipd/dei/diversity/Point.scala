package it.unipd.dei.diversity

import scala.util.Random

class Point(val data: Array[Double]) {

  def dimension: Int = data.length

  def apply(idx: Int): Double = data(idx)

  override def toString: String =
    data.mkString("(", ", ", ")")

}

object Point {

  def apply(data: Double*): Point = new Point(data.toArray)

  def apply(data: Array[Double]): Point = new Point(data)

  def random(dimension: Int): Point =
    Point((0 until dimension).view.map { _ =>
      Random.nextDouble()
    }.toArray)

}