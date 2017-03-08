package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.mllib.Lemmatizer
import it.unipd.dei.experiment.Experiment
import org.apache.spark.ml.feature.{CountVectorizer, IDF, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import it.unipd.dei.diversity.ExperimentUtil.jMap
import org.rogach.scallop.ScallopConf

case class Page(id: Long, vector: Vector)

/**
  * Computes statistics about wikipedia dumps
  */
object WikiStats {

  val TWO_OVER_PI: Double = 2.0 / math.Pi

  def cosineDistance(a: Vector, b: Vector): Double = {
    require(a.numNonzeros > 0, "First vector is zero-valued")
    require(b.numNonzeros > 0, "Second vector is zero-valued")
    require(a.size == b.size)
    var numerator: Double = 0.0
    a.foreachActive { case (i, ca) =>
      numerator += ca * b(i)
    }
    val denomA = Vectors.norm(a, 2)
    val denomB = Vectors.norm(b, 2)
    val res = numerator / (denomA * denomB)
    val dist = TWO_OVER_PI * math.acos(res)
    require(dist < Double.PositiveInfinity, s"Points at infinite distance!! numerator=$numerator normA=$denomA normB=$denomB")
    require(dist > Double.NegativeInfinity, "Points at negative infinite distance!!")
    require(dist != Double.NaN, "Distance NaN")
    dist
  }

  def main(args: Array[String]) {
    val opts = new Opts(args)
    opts.verify()

    val path = opts.input()
    val vocabLength = opts.vocabulary.get.getOrElse(Int.MaxValue)
    val minLength = opts.minLength.get.getOrElse(0)

    val experiment = new Experiment()
    experiment
      .tag("input", path)
      .tag("minimum document length", minLength)
      .tag("vocabulary size", opts.vocabulary.get.orNull)

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

    if (!opts.onlyDistances()) {
      val (catBuckets, catCnts) = raw.select("categories").rdd
        .map(_.getAs[Seq[String]](0).length)
        .histogram(20)
      for ((b, cnt) <- catBuckets.zip(catCnts)) {
        experiment.append("categories per document",
          jMap(
            "num categories" -> b,
            "count" -> cnt))
      }
    }

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
      .setVocabSize(vocabLength)
      .fit(withWords)
    val counts = countVectorizer.transform(withWords)
    val idf = new IDF()
      .setInputCol("tmp-counts")
      .setOutputCol("vector")
      .fit(counts)
    val vectorized = idf.transform(counts).select("id" ,"vector").as[Page].cache()

    if (!opts.onlyDistances()) {
      val (docLenBuckets, docLenCounts) = vectorized.rdd
        .map(_.vector.numNonzeros)
        .histogram(20)
      for ((b, cnt) <- docLenBuckets.zip(docLenCounts)) {
        experiment.append("document length",
          jMap(
            "length" -> b,
            "count" -> cnt))
      }
    }

    val numDocs = vectorized.count()
    val sampleProb = math.min(1.0, 1000.0 / numDocs)
    val sample = vectorized
      .filter(_.vector.numNonzeros > minLength)
      .sample(withReplacement = false, sampleProb)

    val (distBuckets, distCounts) = sample.rdd.cartesian(sample.rdd)
      .filter { case (a, b) => a.id != b.id }
      .map { case (a, b) => cosineDistance(a.vector, b.vector) }
      .histogram(100)

    for ((b, cnt) <- distBuckets.zip(distCounts)) {
      experiment.append("distance distribution length",
        jMap(
          "distance" -> b,
          "count" -> cnt))
    }

    experiment.append("stats",
      jMap("num documents" -> numDocs))

    println(experiment.toSimpleString)
    experiment.saveAsJsonFile(true)
  }

  class Opts(args: Array[String]) extends ScallopConf(args) {

    val input = trailArg[String](required = true)

    val vocabulary = opt[Int]()

    val minLength = opt[Int]()

    val onlyDistances = toggle(default = Some(false))

  }

}
