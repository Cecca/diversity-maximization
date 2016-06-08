package it.unipd.dei.diversity

import org.roaringbitmap.RoaringBitmap
import scala.collection.mutable

trait BagOfWords[T] {

  def wordCounts: mutable.HashMap[T, Int]

  def wordUnion(other: BagOfWords[T]): Iterator[T] =
    this.wordCounts.keySet.union(other.wordCounts.keySet).iterator

  def wordIntersection(other: BagOfWords[T]): Iterator[T] =
    this.wordCounts.keySet.intersect(other.wordCounts.keySet).iterator

  def apply(word: T): Int = wordCounts.getOrElse(word, 0)

}

trait IntBagOfWords extends BagOfWords[Int] {

  private val roaringWords = RoaringBitmap.bitmapOf(wordCounts.keys.toSeq :_*)

  override def wordUnion(o: BagOfWords[Int]): Iterator[Int] = o match {
    case other: IntBagOfWords =>
      val union = RoaringBitmap.or(this.roaringWords, other.roaringWords)
      new Iterator[Int] {
        val it = union.getIntIterator
        override def hasNext: Boolean = it.hasNext
        override def next(): Int = it.next()
      }
    case other =>
      throw new UnsupportedOperationException(
        "Only union between specialized bag of words is supported.")
  }

  override def wordIntersection(o: BagOfWords[Int]): Iterator[Int] = o match {
    case other: IntBagOfWords =>
      val union = RoaringBitmap.and(this.roaringWords, other.roaringWords)
      new Iterator[Int] {
        val it = union.getIntIterator
        override def hasNext: Boolean = it.hasNext
        override def next(): Int = it.next()
      }
    case other =>
      throw new UnsupportedOperationException(
        "Only intersection between specialized bag of words is supported.")
  }

}