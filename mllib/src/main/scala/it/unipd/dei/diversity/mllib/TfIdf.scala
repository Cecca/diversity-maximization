package it.unipd.dei.diversity.mllib

import it.unipd.dei.diversity.mllib.TfIdf.Buffer
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{Identifiable, MLWritable, MLWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering

// TODO Add binary parameter
trait TfIdfParams extends Params with InputCol with OutputCol {

  /**
    * Max size of the vocabulary.
    * CountVectorizer will build a vocabulary that only considers the top
    * vocabSize terms ordered by term frequency across the corpus.
    *
    * Default: 2^18^
    * @group param
    */
  val vocabSize: IntParam =
    new IntParam(this, "vocabSize", "max size of the vocabulary", ParamValidators.gt(0))

  /** @group getParam */
  def getVocabSize: Int = $(vocabSize)

  /**
    * Specifies the minimum number of different documents a term must appear in to be included
    * in the vocabulary.
    * If this is an integer greater than or equal to 1, this specifies the number of documents
    * the term must appear in; if this is a double in [0,1), then this specifies the fraction of
    * documents.
    *
    * Default: 1.0
    * @group param
    */
  val minDF: DoubleParam = new DoubleParam(this, "minDF", "Specifies the minimum number of" +
    " different documents a term must appear in to be included in the vocabulary." +
    " If this is an integer >= 1, this specifies the number of documents the term must" +
    " appear in; if this is a double in [0,1), then this specifies the fraction of documents.",
    ParamValidators.gtEq(0.0))

  /** @group getParam */
  def getMinDF: Double = $(minDF)

  /**
    * Filter to ignore rare words in a document. For each document, terms with
    * frequency/count less than the given threshold are ignored.
    * If this is an integer greater than or equal to 1, then this specifies a count (of times the
    * term must appear in the document);
    * if this is a double in [0,1), then this specifies a fraction (out of the document's token
    * count).
    *
    *
    * Default: 1.0
    * @group param
    */
  val minTF: DoubleParam = new DoubleParam(this, "minTF", "Filter to ignore rare words in" +
    " a document. For each document, terms with frequency/count less than the given threshold are" +
    " ignored. If this is an integer >= 1, then this specifies a count (of times the term must" +
    " appear in the document); if this is a double in [0,1), then this specifies a fraction (out" +
    " of the document's token count). Note that the parameter is only used in transform of" +
    " CountVectorizerModel and does not affect fitting.", ParamValidators.gtEq(0.0))

  /** @group getParam */
  def getMinTF: Double = $(minTF)

  setDefault(vocabSize -> (1 << 18), minDF -> 1.0, minTF -> 1.0)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val typeCandidates = List(new ArrayType(StringType, true), new ArrayType(StringType, false))
    SchemaUtils.checkColumnTypes(schema, $(inputCol), typeCandidates)
    SchemaUtils.appendColumn(schema, $(outputCol), SQLDataTypes.VectorType)
  }

}

/**
  * More efficient (especially for garbage collection pressure) implementation of Tf-Idf
  */
class TfIdf(override val uid: String)
extends Estimator[TfIdfModel] with TfIdfParams {

  def this() = this(Identifiable.randomUID("tf-idf"))

  def setVocabSize(value: Int): this.type = set(vocabSize, value)

  def setMinDF(value: Double): this.type = set(minDF, value)

  def setMinTF(value: Double): this.type = set(minTF, value)

  override def fit(dataset: Dataset[_]): TfIdfModel = {
    transformSchema(dataset.schema, logging = true)
    val vocSize = $(vocabSize)
    val input = dataset.select($(inputCol)).rdd.map(_.getAs[Seq[String]](0))
    val minDf = if ($(minDF) >= 1.0) {
      $(minDF)
    } else {
      $(minDF) * input.cache().count()
    }

    val totalDocs = dataset.sparkSession.sparkContext.longAccumulator("TF-IDF: Document count")

    val wordAndDocCounts: RDD[(String, (Long, Long))] = input.mapPartitions { docs =>
      // Reusable scratch buffers
      val words = Buffer()
      val counts = ArrayBuffer[Long]()
      val distinctWords = ArrayBuffer[String]()

      docs.flatMap { tokens =>
        require(tokens.nonEmpty, "Empty tokens list")
        totalDocs.add(1L)
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
        distinctWords.zip(counts).map { case (word, count) => (word, (count, 1L)) }
      }
    }.reduceByKey { case ((wc1, df1), (wc2, df2)) =>
      (wc1 + wc2, df1 + df2)
    }.filter { case (word, (wc, df)) =>
      df >= minDf
    }.cache()
    val fullVocabSize = wordAndDocCounts.count()

    val vocab = wordAndDocCounts
      .map { case (word, (count, dfCount)) =>
        (word, count)
      }.top(math.min(fullVocabSize, vocSize).toInt)(Ordering.by(_._2))
      .map(_._1)
    require(vocab.length > 0, "The vocabulary size should be > 0. Lower minDF as necessary.")

    val strOrd = Ordering[String]
    val vocabBr = input.sparkContext.broadcast(vocab.toSet)//.sorted(strOrd))

    val numDocs: Long = totalDocs.value
    val idf = wordAndDocCounts
      .filter{ case (word, _) =>
//        java.util.Arrays.binarySearch(vocabBr.value, word, strOrd) >= 0
        vocabBr.value.contains(word)
      }.map { case (word, (count, dfCount)) =>
        val docFreq = dfCount.toLong/numDocs
        (word, math.log((numDocs + 1L) / (docFreq + 1)))
      }.collectAsMap()
    vocabBr.unpersist()

    val idfArr = Array.ofDim[Double](vocab.length)
    for (i <- vocab.indices) {
      idfArr(i) = idf(vocab(i))
    }

    copyValues(new TfIdfModel(uid, vocab, idfArr).setParent(this))
  }


  override def copy(extra: ParamMap): Estimator[TfIdfModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

object TfIdf {


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

}

class TfIdfModel(override val uid: String,
                 val vocabulary: Array[String],
                 val invDocFrequency: Array[Double])
extends Model[TfIdfModel] with MLWritable with TfIdfParams {

  def this(vocabulary: Array[String], invDocFrequency: Array[Double]) =
    this(Identifiable.randomUID("tf-idf"), vocabulary, invDocFrequency)

  override def copy(extra: ParamMap): TfIdfModel = {
    val copied = new TfIdfModel(uid, vocabulary, invDocFrequency).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = ???

  // maps each word to its index in the vocabulary
  private var broadcastVocabDict: Option[Broadcast[Map[String, Int]]] = None
  private var broadcastInvDocFreq: Option[Broadcast[Array[Double]]] = None

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    if (broadcastVocabDict.isEmpty) {
      broadcastVocabDict = Some(
        dataset.sparkSession.sparkContext.broadcast(vocabulary.zipWithIndex.toMap))
    }
    if (broadcastInvDocFreq.isEmpty) {
      broadcastInvDocFreq = Some(dataset.sparkSession.sparkContext.broadcast(invDocFrequency))
    }

    val dictBr = broadcastVocabDict.get
    val invFreqBr = broadcastInvDocFreq.get
    val minTf: Double = getMinTF
    val vectorizer = udf { (document: Seq[String]) =>
      val termCounts = Array.ofDim[Double](dictBr.value.size)
      var tokenCount = 0L
      document.foreach { term =>
        dictBr.value.get(term) match {
          case Some(index) => termCounts(index) += 1.0
          case None => // ignore terms not in the vocabulary
        }
        tokenCount += 1
      }
      val effectiveMinTF = if (minTf >= 1.0) minTf else tokenCount * minTf
      val effectiveCounts = termCounts.view // using a view should avoid allocation
        .zipWithIndex
        .filter(_._2 >= effectiveMinTF)
        .map({case (tf, idx) => (idx, tf*invFreqBr.value(idx))})
      Vectors.sparse(dictBr.value.size, effectiveCounts)
    }
    dataset.withColumn($(outputCol), vectorizer(col($(inputCol))))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

