package it.unipd.dei.diversity

object Distance {

  def euclidean(a: Point, b: Point): Double = {
    require(a.dimension == b.dimension)
    var sum: Double = 0.0
    var i = 0
    while (i<a.dimension) {
      val diff = a(i) - b(i)
      sum += diff*diff
      i += 1
    }
    val res = math.sqrt(sum)
    assert(res < Double.PositiveInfinity, "The distance cannot be infinite! Check your inputs.")
    res
  }

  def jaccard[T](a: Set[T], b: Set[T]): Double = {
    if (a.isEmpty && b.isEmpty) {
      1
    } else {
      val jaccardIndex = a.intersect(b).size.toDouble / a.union(b).size.toDouble
      1 - jaccardIndex
    }
  }

  def jaccard(a: BagOfWords, b: BagOfWords): Double =
    jaccard(a.words, b.words)

}
