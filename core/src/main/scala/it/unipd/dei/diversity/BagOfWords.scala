package it.unipd.dei.diversity

import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable

trait BagOfWords[T] {

  def words: Iterator[T]

  def wordUnion(other: BagOfWords[T]): Iterator[T]

  def wordIntersection(other: BagOfWords[T]): Iterator[T]

  def apply(word: T): Int

}

class ArrayBagOfWords(counts: Seq[(Int, Int)]) extends BagOfWords[Int] {

  val (wordsArray, countsArray): (Array[Int], Array[Int]) = {
    val sorted = counts.sortBy(_._1)
    val (words, cnts) = sorted.unzip
    (words.toArray, cnts.toArray)
  }

  override def words: Iterator[Int] = wordsArray.iterator

  override def apply(word: Int): Int = {
    val idx = wordsArray.indexOf(word)
    if (idx < 0) 0
    else countsArray(idx)
  }

  override def wordIntersection(other: BagOfWords[Int]): Iterator[Int] = ???

  override def wordUnion(other: BagOfWords[Int]): Iterator[Int] = ???

}

object ArrayBagOfWords {

  def apply(counts: Seq[(Int, Int)]): ArrayBagOfWords =
    new ArrayBagOfWords(counts)

  def euclidean(a: ArrayBagOfWords, b: ArrayBagOfWords): Double = {
    var sum = 0.0
    var aIdx = 0
    var bIdx = 0
    while(aIdx < a.wordsArray.length && bIdx < b.wordsArray.length) {
      if (a.wordsArray(aIdx) == b.wordsArray(bIdx)) {
        val diff = a.countsArray(aIdx) - b.countsArray(bIdx)
        sum += diff*diff
        aIdx += 1
        bIdx += 1
      } else if (a.wordsArray(aIdx) < b.wordsArray(bIdx)) {
        val diff = a.countsArray(aIdx)
        sum += diff*diff
        aIdx +=1
      } else {
        val diff = b.countsArray(bIdx)
        sum += diff*diff
        bIdx +=1
      }
    }

    while(aIdx < a.wordsArray.length) {
      val diff = a.countsArray(aIdx)
      sum += diff*diff
      aIdx +=1
    }
    while(bIdx < b.wordsArray.length) {
      val diff = b.countsArray(bIdx)
      sum += diff*diff
      bIdx +=1
    }

    val res = math.sqrt(sum)
    assert(res < Double.PositiveInfinity, "The distance cannot be infinite! Check your inputs.")
    res
  }

}

class MapBagOfWords[T](val wordCounts: mutable.HashMap[T, Int]) extends BagOfWords[T] {

  override def words: Iterator[T] = wordCounts.keysIterator

  override def wordUnion(other: BagOfWords[T]): Iterator[T] =
    this.wordCounts.keySet.union(other.asInstanceOf[MapBagOfWords[T]].wordCounts.keySet).iterator

  override def wordIntersection(other: BagOfWords[T]): Iterator[T] =
    this.wordCounts.keySet.intersect(other.asInstanceOf[MapBagOfWords[T]].wordCounts.keySet).iterator

  override def apply(word: T): Int = wordCounts.getOrElse(word, 0)

}
