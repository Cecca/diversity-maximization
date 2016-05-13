package it.unipd.dei.diversity

import java.util

import scala.util.Random

class Point(val data: Array[Double]) {

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

  override def hashCode(): Int = util.Arrays.hashCode(data)

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

  def randomGaussian(dimension: Int): Point =
    Point((0 until dimension).view.map { _ =>
      Random.nextGaussian()
    }.toArray)

  def zero(dimension: Int): Point =
    Point(Array.fill[Double](dimension)(0.0))

}