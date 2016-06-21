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

  def euclidean[T](bagA: BagOfWords[T], bagB: BagOfWords[T]): Double = (bagA, bagB) match {
    case (a: ArrayBagOfWords, b: ArrayBagOfWords) =>
      ArrayBagOfWords.euclidean(a, b)
    case (a, b) =>
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

  def cosineSimilarity[T](a: BagOfWords[T], b: BagOfWords[T]): Double = {
    val keys = a.wordIntersection(b)
    var numerator: Double = 0.0
    for(k <- keys) {
      numerator += a(k) * b(k)
    }
    val denomA = math.sqrt(a.words.map(w => a(w)*a(w)).sum)
    val denomB = math.sqrt(b.words.map(w => b(w)*b(w)).sum)
    numerator / (denomA * denomB)
  }

  def cosineDistance[T](a: BagOfWords[T], b: BagOfWords[T]): Double = {
    math.acos(cosineSimilarity(a,b)) / math.Pi
  }

}
