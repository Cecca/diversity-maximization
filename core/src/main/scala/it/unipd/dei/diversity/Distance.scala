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

  def euclidean[T](a: BagOfWords[T], b: BagOfWords[T]): Double = {
    val keys = a.wordUnion(b)
    var sum: Double = 0.0
    for (k <- keys) {
      val diff = a(k) - b(k)
      sum += diff*diff
    }
    val res = math.sqrt(sum)
    assert(res < Double.PositiveInfinity, "The distance cannot be infinite! Check your inputs.")
    res
  }
  
  def jaccard[T](a: BagOfWords[T], b: BagOfWords[T]): Double = {
    val denominator = a.wordUnion(b).size
    if (denominator == 0) {
      1.0
    } else {
      1.0 - (a.wordIntersection(b).size.toDouble / denominator.toDouble)
    }
  }

}
