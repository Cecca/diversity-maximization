package it.unipd.dei.diversity

object Utils {

  def pairs[T](points: IndexedSeq[T]): Iterator[(T, T)] =
    {
      for {
        i <- points.indices.iterator
        j <- ((i+1) until points.size).iterator
      } yield (points(i), points(j))
    }

  def minDistance[T](points: IndexedSeq[T],
                     distance: (T, T) => Double): Double = {
    require(points.length >= 2, "At least two points are needed")
    pairs(points).map{case (a, b) => distance(a,b)}.min
  }

  def maxMinDistance[T](pointsA: IndexedSeq[T],
                        pointsB: IndexedSeq[T],
                        distance: (T, T) => Double): Double = {
    require(pointsA.nonEmpty && pointsB.nonEmpty, "At least two points are needed")
    pointsA.map { p1 =>
      pointsB.map { p2 =>
        distance(p1, p2)
      }.min
    }.max
  }

}
