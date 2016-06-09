package it.unipd.dei.diversity

import org.roaringbitmap.RoaringBitmap
import scala.collection.mutable

trait BagOfWords[T] {

  def words: Iterator[T]

  def wordUnion(other: BagOfWords[T]): Iterator[T]

  def wordIntersection(other: BagOfWords[T]): Iterator[T]

  def apply(word: T): Int

}

class MapBagOfWords[T](val wordCounts: mutable.HashMap[T, Int]) extends BagOfWords[T] {

  override def words: Iterator[T] = wordCounts.keysIterator

  override def wordUnion(other: BagOfWords[T]): Iterator[T] =
    this.wordCounts.keySet.union(other.asInstanceOf[MapBagOfWords[T]].wordCounts.keySet).iterator

  override def wordIntersection(other: BagOfWords[T]): Iterator[T] =
    this.wordCounts.keySet.intersect(other.asInstanceOf[MapBagOfWords[T]].wordCounts.keySet).iterator

  override def apply(word: T): Int = wordCounts.getOrElse(word, 0)

}

class IntBagOfWords(val wordCounts: mutable.HashMap[Int, Int]) extends BagOfWords[Int] {

  private val roaringWords = RoaringBitmap.bitmapOf(wordCounts.keys.toSeq :_*)

  override def apply(word: Int): Int = wordCounts.getOrElse(word, 0)

  override def words: Iterator[Int] = wordCounts.keysIterator

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