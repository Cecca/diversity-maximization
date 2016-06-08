package it.unipd.dei.diversity

trait BagOfWords[T] {

  def wordCounts: Map[T, Int]

  def words: Set[T] = wordCounts.keySet

  def wordUnion(other: BagOfWords[T]): Iterator[T] =
    this.words.union(other.words).iterator

  def wordIntersection(other: BagOfWords[T]): Iterator[T] =
    this.words.intersect(other.words).iterator

  def apply(word: T): Int = wordCounts.getOrElse(word, 0)

}
