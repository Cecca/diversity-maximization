package it.unipd.dei.diversity.mllib

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering

/**
  * Specializes [[org.apache.spark.ml.feature.CountVectorizer]] for large volumes of data
  */
class SortingCountVectorizer extends CountVectorizer {

  /**
    * A growable buffer of strings whose content can be sorted in place
    */
  private class Buffer(private var _backingArray: Array[String]) {

    var _size: Int = 0

    def size: Int = _size

    def apply(i: Int): String = {
      if (i >= _size) throw new IndexOutOfBoundsException(s"$i >= ${_size}")
      _backingArray(i)
    }

    def sortInPlace(): Unit = {
      java.util.Arrays.sort(_backingArray, 0, _size, Ordering[String])
    }

    def clear(): Unit = _size = 0

    def ensureCapacity(c: Int) = {
      if (_backingArray.length < c) {
        val largerArr = Array.ofDim[String](c)
        System.arraycopy(_backingArray, 0, largerArr, 0, _size)
        _backingArray = largerArr
      }
    }

    def appendAll(seq: Seq[String]): Unit = {
      ensureCapacity(_size + seq.size)
      for (e <- seq) {
        _backingArray(_size) = e
        _size += 1
      }
    }

  }

  private object Buffer {
    def apply(): Buffer = new Buffer(Array[String]())
  }

  override def fit(dataset: Dataset[_]): CountVectorizerModel = {
    transformSchema(dataset.schema, logging = true)
    val vocSize = $(vocabSize)
    val input = dataset.select($(inputCol)).rdd.map(_.getAs[Seq[String]](0))
    val minDf = if ($(minDF) >= 1.0) {
      $(minDF)
    } else {
      $(minDF) * input.cache().count()
    }
    val wordCounts: RDD[(String, Long)] = input.mapPartitions { docs =>
      // Reusable scratch buffers
      val words = Buffer()
      val counts = ArrayBuffer[Long]()
      val distinctWords = ArrayBuffer[String]()

      docs.flatMap { tokens =>
        require(tokens.nonEmpty, "Empty tokens list")
        words.clear()
        words.appendAll(tokens)
        words.sortInPlace()

        counts.clear()
        counts.sizeHint(words.size)
        var countsIdx = 0

        distinctWords.clear()
        distinctWords.sizeHint(words.size)
        distinctWords.append(words(0))
        counts.append(1L)

        var lastWord = words(0)
        var wordsIdx = 1 // start from the second word, the first is counted manually
        while (wordsIdx < words.size) {
          if (words(wordsIdx).equals(lastWord)) {
            counts(countsIdx) += 1
          } else {
            countsIdx += 1
            counts.append(1L)
            distinctWords.append(words(wordsIdx))
            lastWord = words(wordsIdx)
          }
          wordsIdx += 1
        }
        distinctWords.zip(counts).map { case (word, count) => (word, (count, 1)) }
      }
    }.reduceByKey { case ((wc1, df1), (wc2, df2)) =>
      (wc1 + wc2, df1 + df2)
    }.filter { case (word, (wc, df)) =>
      df >= minDf
    }.map { case (word, (count, dfCount)) =>
      (word, count)
    }.cache()
    val fullVocabSize = wordCounts.count()

    val vocab = wordCounts
      .top(math.min(fullVocabSize, vocSize).toInt)(Ordering.by(_._2))
      .map(_._1)

    require(vocab.length > 0, "The vocabulary size should be > 0. Lower minDF as necessary.")
    copyValues(new CountVectorizerModel(uid, vocab).setParent(this))
  }


}
