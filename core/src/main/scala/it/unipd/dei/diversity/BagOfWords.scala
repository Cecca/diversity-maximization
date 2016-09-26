// diversity-maximization: Diversity maximization in Streaming and MapReduce
// Copyright (C) 2016  Matteo Ceccarello <ceccarel@dei.unipd.it>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package it.unipd.dei.diversity

import scala.collection.mutable

trait BagOfWords[T] {

  def words: Iterator[T]

  def wordUnion(other: BagOfWords[T]): Iterator[T] =
    words.toSet.union(other.words.toSet).iterator

  def wordIntersection(other: BagOfWords[T]): Iterator[T] =
    words.toSet.intersect(other.words.toSet).iterator

  def apply(word: T): Double

}

// TODO: Add an additional constructor that directly accepts the words and counts arrays
class ArrayBagOfWords(val wordsArray: Array[Int],
                      val scoresArray: Array[Double])
extends BagOfWords[Int] with Serializable {

  def this(arrayPair: (Array[Int], Array[Double])) {
    this(arrayPair._1, arrayPair._2)
  }

  def this(counts: Seq[(Int, Double)]) = {
    this(ArrayBagOfWords.buildArrayPair(counts))
  }

  override def words: Iterator[Int] = wordsArray.iterator

  override def apply(word: Int): Double = {
    val idx = wordsArray.indexOf(word)
    if (idx < 0) 0
    else scoresArray(idx)
  }

  override def toString: String =
    wordsArray.zip(scoresArray).toMap.toString

}

object ArrayBagOfWords {

  def buildArrayPair(counts: Seq[(Int, Double)]): (Array[Int], Array[Double]) = {
    val sorted = counts.sortBy(_._1)
    val (words, cnts) = sorted.unzip
    (words.toArray, cnts.toArray)
  }

  def apply(counts: Seq[(Int, Double)]): ArrayBagOfWords =
    new ArrayBagOfWords(counts)

  def euclidean(a: ArrayBagOfWords, b: ArrayBagOfWords): Double = {
    var sum = 0.0
    var aIdx = 0
    var bIdx = 0
    while(aIdx < a.wordsArray.length && bIdx < b.wordsArray.length) {
      if (a.wordsArray(aIdx) == b.wordsArray(bIdx)) {
        val diff = a.scoresArray(aIdx) - b.scoresArray(bIdx)
        sum += diff*diff
        aIdx += 1
        bIdx += 1
      } else if (a.wordsArray(aIdx) < b.wordsArray(bIdx)) {
        val diff = a.scoresArray(aIdx)
        sum += diff*diff
        aIdx +=1
      } else {
        val diff = b.scoresArray(bIdx)
        sum += diff*diff
        bIdx +=1
      }
    }

    while(aIdx < a.wordsArray.length) {
      val diff = a.scoresArray(aIdx)
      sum += diff*diff
      aIdx +=1
    }
    while(bIdx < b.wordsArray.length) {
      val diff = b.scoresArray(bIdx)
      sum += diff*diff
      bIdx +=1
    }

    val res = math.sqrt(sum)
    assert(res < Double.PositiveInfinity, "The distance cannot be infinite! Check your inputs.")
    res
  }

  def cosineSimilarity(a: ArrayBagOfWords, b: ArrayBagOfWords): Double = {
    var numerator = 0.0
    var aIdx = 0
    var bIdx = 0
    while(aIdx < a.wordsArray.length && bIdx < b.wordsArray.length) {
      if (a.wordsArray(aIdx) == b.wordsArray(bIdx)) {
        numerator += a.scoresArray(aIdx) * b.scoresArray(bIdx)
        aIdx += 1
        bIdx += 1
      } else if (a.wordsArray(aIdx) < b.wordsArray(bIdx)) {
        aIdx += 1
      } else {
        bIdx += 1
      }
    }

    var denominatorA = 0.0
    aIdx = 0
    while (aIdx < a.scoresArray.length) {
      val v = a.scoresArray(aIdx)
      denominatorA += v*v
      aIdx += 1
    }
    var denominatorB = 0.0
    bIdx = 0
    while (bIdx < b.scoresArray.length) {
      val v = b.scoresArray(bIdx)
      denominatorB += v*v
      bIdx += 1
    }

    val res = numerator / ( math.sqrt(denominatorA) * math.sqrt(denominatorB) )
    // See the comment in Distance.cosineSimilarity for the motivation of the
    // following operation.
    math.min(1.0, res)
  }

  def cosineDistance(a: ArrayBagOfWords, b: ArrayBagOfWords): Double = {
    2*math.acos(cosineSimilarity(a,b)) / math.Pi
  }

}

class MapBagOfWords[T](val wordCounts: mutable.HashMap[T, Double]) extends BagOfWords[T] {

  override def words: Iterator[T] = wordCounts.keysIterator

  override def wordUnion(other: BagOfWords[T]): Iterator[T] =
    this.wordCounts.keySet.union(other.asInstanceOf[MapBagOfWords[T]].wordCounts.keySet).iterator

  override def wordIntersection(other: BagOfWords[T]): Iterator[T] =
    this.wordCounts.keySet.intersect(other.asInstanceOf[MapBagOfWords[T]].wordCounts.keySet).iterator

  override def apply(word: T): Double = wordCounts.getOrElse(word, 0.0)

  override def toString: String = wordCounts.toString

}
