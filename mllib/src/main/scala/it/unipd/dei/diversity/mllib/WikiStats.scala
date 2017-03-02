package it.unipd.dei.diversity.mllib

import org.apache.spark.ml.feature.{CountVectorizer, IDF, StopWordsRemover}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession


/**
  * Computes statistics about wikipedia dumps
  */
object WikiStats {

  def main(args: Array[String]) {
    val path = args(0)

    val spark = SparkSession.builder()
      .appName("WikiStats")
      .getOrCreate()

    val jsonRdd = spark.sparkContext
      .wholeTextFiles(path)
      .flatMap { case (_, text) => text.split("\n") }
    val raw = spark.read.json(jsonRdd)
      .repartition(spark.sparkContext.defaultParallelism)
      .persist()

    val categoriesCounts = raw.select("categories").rdd
      .map(_.getAs[Seq[String]](0))
      .map { cats => (cats.length, 1) }
      .reduceByKey(_ + _)
      .sortByKey() // RDD of (category length, count) pairs
      .collect()
    println("Number of categories -> number of pages")
    println(categoriesCounts.mkString("\n"))


    // Stats on text lengths
    val lemmatizer = new Lemmatizer()
      .setInputCol("text")
      .setOutputCol("lemmas")
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("lemmas")
      .setOutputCol("words")
      .setCaseSensitive(false)
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
    val withWords = stopWordsRemover.transform(lemmatizer.transform(raw))

    val countVectorizer = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("tmp-counts")
      //      .setVocabSize(5000) // Restrict to the 5000 most-frequent words
      .fit(withWords)
    val counts = countVectorizer.transform(withWords)
    val idf = new IDF()
      .setInputCol("tmp-counts")
      .setOutputCol("tf-idf")
      .fit(counts)
    val vectorized = idf.transform(counts).drop("tmp-counts")

    val numWords = vectorized.select("tf-idf").rdd
      .map(_.getAs[Vector](0))
      .map { vec => (vec.numNonzeros, 1) }
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()

    println(s"Relevant word distribution (Vocabulary size: ${countVectorizer.vocabulary.size} words)")
    println(numWords.mkString("\n"))
  }

}
