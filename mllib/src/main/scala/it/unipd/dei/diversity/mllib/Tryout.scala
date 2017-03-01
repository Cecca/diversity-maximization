package it.unipd.dei.diversity.mllib

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, IDF, StopWordsRemover}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CategorizedBow(id: Long,
                          categories: Array[String],
                          title: String,
                          counts: Vector)


object Tryout {

  def textToWords(dataset: DataFrame): DataFrame = {
    val lemmatizer = new Lemmatizer()
      .setInputCol("text")
      .setOutputCol("lemmas")
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("lemmas")
      .setOutputCol("words")
      .setCaseSensitive(false)
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
    stopWordsRemover.transform(lemmatizer.transform(dataset)).drop("lemmas")
  }

  def wordsToVec(dataset: DataFrame): DataFrame = {
    val countVectorizer = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("tmp-counts")
      .setVocabSize(5000) // Restrict to the 5000 most-frequent words
      .fit(dataset)
    val counts = countVectorizer.transform(dataset)

    val idf = new IDF()
      .setInputCol("tmp-counts")
      .setOutputCol("tf-idf")
      .fit(counts)
    idf.transform(counts).drop("tmp-counts")
  }

  def main(args: Array[String]) {

    val path = "/data/matteo/Wikipedia/enwiki-20170120/AA/wiki_00.bz2"

    val conf = new SparkConf(loadDefaults = true)
    val spark = SparkSession.builder()
      .appName("tryout")
      .master("local")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val raw = spark
      .read.json(path)
      .select("id", "categories", "title", "text")

    val words = textToWords(raw)
    val vecs = wordsToVec(words)
      .withColumnRenamed("tf-idf", "counts")
      .select("id", "title", "categories", "counts")
      .as[CategorizedBow]

    val categories = new CategoriesToIndex()
      .setInputCol("categories")
      .setOutputCol("cats")
      .fit(vecs)
    //    println(s"Categories are:\n${categories.getCategories.mkString("\n")}")
    val catVecs = categories.transform(vecs).drop("categories")

    catVecs.explain(true)
    catVecs.show()

    val inverse = categories
      .inverse
      .setInputCol("cats")
      .setOutputCol("origCats")

    inverse.transform(catVecs).show()
  }

}

