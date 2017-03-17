package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.ExperimentUtil.jMap
import it.unipd.dei.diversity.IndexedSubset
import it.unipd.dei.diversity.matroid.TransversalMatroid
import it.unipd.dei.diversity.mllib.TfIdf
import it.unipd.dei.experiment.Experiment
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

import scala.collection.mutable.ArrayBuffer

case class Page(id: Long, categories: Array[String], vector: Vector)

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
    val res = math.min(numerator / (denomA * denomB), 1.0)
    val dist = TWO_OVER_PI * math.acos(res)
    require(dist != Double.NaN, "Distance NaN")
    require(dist < Double.PositiveInfinity,
      s"Points at infinite distance!! " +
        s"numerator=$numerator normA=$denomA normB=$denomB \n" +
        s"vecA: $a\n" +
        s"vecB: $b")
    require(dist > Double.NegativeInfinity, "Points at negative infinite distance!!")
    dist
  }

  def cosineDistance(a: Page, b: Page): Double = cosineDistance(a.vector, b.vector)

  def diversity(points: IndexedSubset[Page],
                distance: (Page, Page) => Double): Double = {
    var sum = 0.0
    for (i <- points.supersetIndices; j <- points.supersetIndices) {
      if (i < j) {
        sum += distance(points.superSet(i), points.superSet(j))
      }
    }
    sum
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
      .tag("sample size", opts.sampleSize())

    val spark = SparkSession.builder()
      .appName("WikiStats")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val dataset = spark.read.parquet(path)
      .select("id", "lemmas", "categories").cache()

    if (opts.categories()) {
      val (catBuckets, catCnts) = dataset.select("categories").rdd
        .map(_.getAs[Seq[String]](0).length)
        .histogram(20)
      for ((b, cnt) <- catBuckets.zip(catCnts)) {
        experiment.append("categories per document",
          jMap(
            "num categories" -> b,
            "count" -> cnt))
      }
    }

    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("lemmas")
      .setOutputCol("words")
      .setCaseSensitive(false)
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
    val withWords = stopWordsRemover.transform(dataset).filter("size(words) > 0")

    val tfIdf = new TfIdf()
      .setInputCol("words")
      .setOutputCol("vector")
      .setVocabSize(vocabLength)
      .fit(withWords)
    val vectorized = tfIdf.transform(withWords)
      .select("id", "categories", "vector").as[Page]
      .filter(p => p.vector.numNonzeros > 0 && p.vector.numNonzeros >= minLength)
      .cache()

    val numDocs = vectorized.count()
    experiment.append("stats",
      jMap("num documents" -> numDocs))


    if (opts.docLength()) {
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

    if (opts.distances()) {
      val sampleProb = math.min(1.0, opts.sampleSize() / numDocs.toDouble)
      println(s"Sampling with probability $sampleProb (from $numDocs documents)")
      val sample = vectorized
        .sample(withReplacement = false, sampleProb)
        .persist()

      // materialize the samples
      val sampleCnt = sample.count()
      println(s"Samples taken $sampleCnt")
      require(sampleCnt > 0, "No samples taken!")

      val (distBuckets, distCounts) = sample.rdd.cartesian(sample.rdd)
        .coalesce(spark.sparkContext.defaultParallelism)
        .filter { case (a, b) => a.id != b.id }
        .map { case (a, b) => cosineDistance(a.vector, b.vector) }
        .histogram(100)

      for ((b, cnt) <- distBuckets.zip(distCounts)) {
        experiment.append("distance distribution length",
          jMap(
            "distance" -> b,
            "count" -> cnt))
      }
    }

    if (opts.sampleDiversity.supplied) {
      require(opts.sampleSize.supplied)
      require(opts.k.supplied)
      val k = opts.k()
      val categories = vectorized.rdd.flatMap(_.categories).distinct().collect()
      val numCategories = categories.length
      println(s"There are $numCategories categories")
      require(numCategories > 0, "There are no categories!!!")
      val matroid = new TransversalMatroid[Page, String](categories, _.categories)
      val diversities = new ArrayBuffer[(Int, Double)]()
      for (i <- 0 until opts.sampleDiversity()) {
        val sampleProb = math.min(1.0, opts.sampleSize() / numDocs.toDouble)
        println(s"Sampling with probability $sampleProb (from $numDocs documents)")
        val sample: Array[Page] = vectorized
          .sample(withReplacement = false, sampleProb)
          .collect()
        val is = matroid.independentSetOfSize(sample, k)
        val div = diversity(is, cosineDistance)
        diversities.append((i, div))
      }
      val numEdgesInSolution = k * (k - 1) / 2
      println(s"The solution contains $numEdgesInSolution edges")
      for ((i, d) <- diversities) {
        experiment.append("diversity",
          jMap(
            "sample-idx" -> i,
            "diversity" -> d,
            "diversity normalized" -> d / numEdgesInSolution.toDouble))
      }
    }

    println(experiment.toSimpleString)
    experiment.saveAsJsonFile(true)
    spark.sparkContext.cancelAllJobs()
    spark.stop()
  }

  class Opts(args: Array[String]) extends ScallopConf(args) {

    val input = trailArg[String](required = true)

    val vocabulary = opt[Int]()

    val minLength = opt[Int]()

    val distances = toggle(default = Some(false))

    val sampleSize = opt[Long](default = Some(1000L))

    val categories = toggle(default = Some(false))

    val docLength = toggle(default = Some(false))

    val k = opt[Int]()

    val sampleDiversity = opt[Int](
      descr = "Compute the remote-clique diversity under transversal" +
        " matroid constraints using the given number of samples.")
  }

}
