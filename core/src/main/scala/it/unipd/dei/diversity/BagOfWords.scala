package it.unipd.dei.diversity

trait BagOfWords[T] {

  def wordCounts: Map[T, Int]

  def words: Set[T] = wordCounts.keySet

}
