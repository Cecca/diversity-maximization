package it.unipd.dei.diversity.mllib

import org.apache.spark.ml.feature.{CountVectorizer, IDF, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._

case class Page(id: Long, vector: Vector)

/**
  * Computes statistics about wikipedia dumps
  */
object WikiStats {

  def cosineDistance(a: Vector, b: Vector): Double = {
    require(a.size == b.size)
    var numerator: Double = 0.0
    a.foreachActive { case (i, ca) =>
      numerator += ca * b(i)
    }
    val denomA = Vectors.norm(a, 2)
    val denomB = Vectors.norm(b, 2)
    val res = numerator / (denomA * denomB)
    2 * math.acos(res) / math.Pi
  }

  def main(args: Array[String]) {
    val path = args(0)

    val spark = SparkSession.builder()
      .appName("WikiStats")
      .getOrCreate()
    import spark.implicits._

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
      .setVocabSize(5000) // Restrict to the 5000 most-frequent words
      .fit(withWords)
    val counts = countVectorizer.transform(withWords)
    val idf = new IDF()
      .setInputCol("tmp-counts")
      .setOutputCol("vector")
      .fit(counts)
    val vectorized = idf.transform(counts).select("id" ,"vector").as[Page].cache()

    val numDocs = vectorized.count()

    val (bounds, numWords) = vectorized.rdd
      .map(_.vector.numNonzeros)
      .histogram(10)

    println(s"Relevant word distribution (Vocabulary size: ${countVectorizer.vocabulary.size} words)")
    println(bounds.zip(numWords).mkString("\n"))

    val sampleProb = math.min(1.0, 1000.0 / numDocs)
    val Array(dataA, dataB) = vectorized.randomSplit(Array(0.5, 0.5))
    val sampleA = dataA.sample(withReplacement = false, sampleProb)
    val sampleB = dataB.sample(withReplacement = false, sampleProb)

    val (buckets, distances) = sampleA.rdd.cartesian(sampleB.rdd)
      .filter { case (a, b) => a.id != b.id}
      .map { case (a, b) => cosineDistance(a.vector, b.vector) }
      .histogram(10)

    println("Distances histogram (from samples)")
    println(s"${buckets.zip(distances).mkString("\n")}")
  }

}
