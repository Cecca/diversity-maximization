package it.unipd.dei.diversity

class Point(val data: Array[Double]) {

  def dimension: Int = data.length

  def apply(idx: Int): Double = data(idx)

  override def toString: String =
    data.mkString("(", ", ", ")")

}

object Point {

  def apply(data: Double*): Point = new Point(data.toArray)

}