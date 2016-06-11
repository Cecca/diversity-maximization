package it.unipd.dei.diversity

import it.unipd.dei.experiment.Experiment
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object MainPointsLocalSearch {

  def main(args: Array[String]) {

    // Read command line options
    val opts = new Conf(args)
    opts.verify()
    val sourcesList = opts.source().split(",")
    val dimList = opts.spaceDimension().split(",").map{_.toInt}
    val kList = opts.k().split(",").map{_.toInt}
    val epsilonList = opts.epsilon().split(",").map{_.toDouble}
    val numPointsList = opts.numPoints().split(",").map{_.toInt}
    val runs = opts.runs()
    val approxRuns = opts.approxRuns()
    val directory = opts.directory()

    val distance: (Point, Point) => Double = Distance.euclidean
    val diversity: (IndexedSeq[Point], (Point, Point) => Double) => Double =
      Diversity.clique[Point]

    // Set up Spark lazily, it will be initialized only if the algorithm needs it.
    lazy val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("MapReduce coresets")
    lazy val sc = new SparkContext(sparkConfig)

    // Cycle through parameter configurations
    for {
      r <- 0 until runs
      sourceName <- sourcesList
      dim      <- dimList
      k        <- kList
      n        <- numPointsList
      epsilon  <- epsilonList
    } {
      val experiment = new Experiment()
        .tag("experiment", "Points")
        .tag("version", BuildInfo.version)
        .tag("git-revision", BuildInfo.gitRevision)
        .tag("git-revcount", BuildInfo.gitRevCount)
        .tag("source", sourceName)
        .tag("space-dimension", dim)
        .tag("k", k)
        .tag("epsilon", epsilon)
        .tag("num-points", n)
        .tag("computeFarthest", false)
        .tag("computeMatching", true)

      val coreset: Coreset[Point] = {
        val parallelism = sc.defaultParallelism
        experiment.tag("parallelism", parallelism)
        val points = sc.objectFile[Point](
          DatasetGenerator.filename(directory, sourceName, dim, n, k),
          parallelism)
        Algorithm.localSearch(points, k, epsilon, distance, diversity, experiment)
      }

      Approximation.approximate(
        coreset, k, distance, computeFarthest = false, computeMatching = true, approxRuns, experiment)

      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

  }

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val source = opt[String](default = Some("versor"))

    lazy val spaceDimension = opt[String](default = Some("2"))

    lazy val k = opt[String](required = true)

    lazy val numPoints = opt[String](required = true)

    lazy val runs = opt[Int](default = Some(1))

    lazy val approxRuns = opt[Int](default = Some(1))

    lazy val directory = opt[String](default = Some("/tmp"))

    lazy val epsilon = opt[String](default = Some("1.0"))

  }


}
