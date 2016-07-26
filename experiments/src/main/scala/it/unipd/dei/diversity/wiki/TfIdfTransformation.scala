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

package it.unipd.dei.diversity.wiki

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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
    val numDocs = data.persist(StorageLevel.MEMORY_AND_DISK).count()
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
