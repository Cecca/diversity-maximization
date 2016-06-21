package it.unipd.dei.diversity.wiki

import org.apache.spark.rdd.RDD
import scala.language.implicitConversions

/**
  * Perform rescaling of Bag of Words using Term Frequency
  * and Inverse Document Frequency. For reference, see
  *
  * http://nlp.stanford.edu/IR-book/html/htmledition/term-frequency-and-weighting-1.html
  */
object TfIdfTransformation {

  def apply(data: RDD[WikiBagOfWords]): RDD[WikiBagOfWords] = {
    // 1. Compute a inverse document frequency table to be broadcasted later.
    val numDocs = data.cache().count()
    val localInverseDocFreqs = data.flatMap { bow =>
      bow.words.map(w => (w, 1))
    }.reduceByKey(_ + _).mapValues { docFrequency =>
      math.log(numDocs.toDouble/ docFrequency.toDouble)
    }.collectAsMap()
    println(s"There are a total of ${localInverseDocFreqs.size} words in the dataset")
    val inverseDocFreqs = data.sparkContext.broadcast(localInverseDocFreqs)

    // 2. rescale the terms
    data.map { bow =>
      val newScores = Array.ofDim[Double](bow.wordsArray.length)
      var i = 0
      while (i < bow.wordsArray.length) {
        newScores(i) = bow.scoresArray(i) * inverseDocFreqs.value(bow.wordsArray(i))
        i += 1
      }
      new WikiBagOfWords(bow.title, bow.categories, bow.wordsArray.clone(), newScores)
    }
  }

  implicit def rddToTfIdfTransformation(rdd: RDD[WikiBagOfWords]): TfIdfTransformation =
    new TfIdfTransformation(rdd)

}

class TfIdfTransformation(val rdd: RDD[WikiBagOfWords]) {

  /**
    * Rescale the word scores by Term Frequency - Inverse Document Frequency
    */
  def rescaleTfIdf(): RDD[WikiBagOfWords] = TfIdfTransformation(rdd)

}
