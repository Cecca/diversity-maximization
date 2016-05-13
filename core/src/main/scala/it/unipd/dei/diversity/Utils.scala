package it.unipd.dei.diversity

object Utils {

  def minDistance[T](points: IndexedSeq[T],
                     distance: (T, T) => Double): Double = {
    require(points.length >= 2, "At least two points are needed")
    points.flatMap { p1 =>
      points.flatMap { p2 =>
        if (p1 != p2) {
          Iterable(distance(p1, p2))
        } else {
          Iterable.empty
        }
      }
    }.min
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
