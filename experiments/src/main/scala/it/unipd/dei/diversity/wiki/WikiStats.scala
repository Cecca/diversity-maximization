package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.ExperimentUtil.jMap
import it.unipd.dei.diversity.matroid.{TransversalMatroid, WikiPage}
import it.unipd.dei.diversity.{IndexedSubset, SerializationUtils}
import it.unipd.dei.experiment.Experiment
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.rogach.scallop.ScallopConf

import scala.collection.mutable.ArrayBuffer



/**
  * Computes statistics about wikipedia dumps
  */
object WikiStats {

  val TWO_OVER_PI: Double = 2.0 / math.Pi
  val ONE_OVER_PI: Double = 1.0 / math.Pi

  def cosineDistance(a: Vector, b: Vector, normalization: Double): Double = {
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
    val dist = normalization * math.acos(res)
    require(dist != Double.NaN, "Distance NaN")
    require(dist < Double.PositiveInfinity,
      s"Points at infinite distance!! " +
        s"numerator=$numerator normA=$denomA normB=$denomB \n" +
        s"vecA: $a\n" +
        s"vecB: $b")
    require(dist > Double.NegativeInfinity, "Points at negative infinite distance!!")
    dist
  }

  def cosineDistanceOnlyPositive(a: WikiPage, b: WikiPage): Double = {
    val d = cosineDistance(a.vector, b.vector, TWO_OVER_PI)
    require(d <= 1.0, s"Cosine distance greater than one! a=`${a.title}`, b=`${b.title}`")
    d
  }

  def cosineDistanceAlsoNegative(a: WikiPage, b: WikiPage): Double = {
    val d = cosineDistance(a.vector, b.vector, ONE_OVER_PI)
    require(d <= 1.0, s"Cosine distance greater than one ($d)! a=`${a.title}`, b=`${b.title}`")
    d
  }

  def euclidean(a: WikiPage, b: WikiPage): Double = Vectors.sqdist(a.vector, b.vector)

  def manhattan(a: WikiPage, b: WikiPage): Double = {
    var i = 0
    val size = a.vector.size
    var sum = 0.0
    while (i < size) {
      sum += math.abs(a.vector(i) - b.vector(i))
      i += 1
    }
    sum
  }

  val distanceFunctions: Map[String, (WikiPage, WikiPage) => Double] = Map(
    "cosine-only-positive" -> cosineDistanceOnlyPositive,
    "cosine-also-negative" -> cosineDistanceAlsoNegative,
    "euclidean" -> euclidean,
    "manhattan" -> manhattan
  )

  def diversity(points: IndexedSubset[WikiPage],
                distance: (WikiPage, WikiPage) => Double): Double = {
    var sum = 0.0
    for (i <- points.supersetIndices; j <- points.supersetIndices) {
      if (i < j) {
        sum += distance(points.superSet(i), points.superSet(j))
      }
    }
    sum
  }

  def loadDataset(spark: SparkSession, opts: Opts): Dataset[WikiPage] = {
    import spark.implicits._
    spark.read.parquet(opts.input())
      .select("id", "title", "categories", "vector").as[WikiPage].cache()
  }

  def main(args: Array[String]) {
    val opts = new Opts(args)
    opts.verify()

    val path = opts.input()
    val distanceFn: (WikiPage, WikiPage) => Double =
      distanceFunctions(opts.distanceFunction.get.map{ desc =>
        if ("cosine".equals(desc)) {
          if (SerializationUtils.metadata(opts.input()).get("representation").get.equals("word2vec")) "cosine-also-negative"
          else "cosine-only-positive"
        } else {
          desc
        }
      }.get)

    val experiment = new Experiment()
    experiment
      .tag("input", path)
      .tag("distance function", opts.distanceFunction())
      .tag("sample size", opts.sampleSize())

    val spark = SparkSession.builder()
      .appName("WikiStats")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val dataset = loadDataset(spark, opts).cache()

    val numDocs = dataset.count()
    experiment.append("stats",
      jMap("num documents" -> numDocs))

    if (opts.categories()) {
      import spark.implicits._
      val cats = dataset.select("categories")
        .as[Seq[String]]
        .rdd
        .flatMap(cs => cs.map(c => (c, 1)))
        .reduceByKey(_ + _)
        .sortBy(_._2)
        .cache()
      val categoriesCount = cats.count()
      println(s"There are $categoriesCount categories")

      for ((c, cnt) <- cats.toLocalIterator) {
        experiment.append("categories", jMap(
          "category" -> c,
          "count" -> cnt
        ))
      }
    }
    
    if (opts.distances()) {
      val sampleProb = math.min(1.0, opts.sampleSize() / numDocs.toDouble)
      println(s"Sampling with probability $sampleProb (from $numDocs documents)")
      val sample = dataset
        .sample(withReplacement = false, sampleProb)
        .persist()

      // materialize the samples
      val sampleCnt = sample.count()
      println(s"Samples taken $sampleCnt")
      require(sampleCnt > 0, "No samples taken!")

      val (distBuckets, distCounts) = sample.rdd.cartesian(sample.rdd)
        .coalesce(spark.sparkContext.defaultParallelism)
        .filter { case (a, b) => a.id != b.id }
        .map { case (a, b) => distanceFn(a, b) }
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
      val categories = dataset.rdd.flatMap(_.categories).distinct().collect()
      val numCategories = categories.length
      println(s"There are $numCategories categories")
      require(numCategories > 0, "There are no categories!!!")
      val matroid = new TransversalMatroid[WikiPage, String](categories, _.categories)
      val diversities = new ArrayBuffer[(Int, Double)]()
      for (i <- 0 until opts.sampleDiversity()) {
        val sampleProb = math.min(1.0, opts.sampleSize() / numDocs.toDouble)
        println(s"Sampling with probability $sampleProb (from $numDocs documents)")
        val sample: Array[WikiPage] = dataset
          .sample(withReplacement = false, sampleProb)
          .collect()
        val is = matroid.independentSetOfSize(sample, k)
        val div = diversity(is, distanceFn)
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

    val distances = toggle(default = Some(false))

    val distanceFunction = opt[String](default = Some("cosine"))

    val sampleSize = opt[Long](default = Some(1000L))

    val categories = toggle(default = Some(false))

    val k = opt[Int]()

    val sampleDiversity = opt[Int](
      descr = "Compute the remote-clique diversity under transversal" +
        " matroid constraints using the given number of samples.")
  }

}
