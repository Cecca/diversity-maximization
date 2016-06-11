package it.unipd.dei.diversity

import it.unipd.dei.experiment.Experiment
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object MainPoints {

  def main(args: Array[String]) {

    // Read command line options
    val opts = new Conf(args)
    opts.verify()
    val algorithm = opts.algorithm()
    val sourcesList = opts.source().split(",")
    val dimList = opts.spaceDimension().split(",").map{_.toInt}
    val kList = opts.delegates().split(",").map{_.toInt}
    val numPointsList = opts.numPoints().split(",").map{_.toInt}
    val kernelSizeList = opts.kernelSize().split(",").map{_.toInt}
    val runs = opts.runs()
    val computeFarthest = opts.farthest()
    val computeMatching = opts.matching()
    val directory = opts.directory()

    val distance: (Point, Point) => Double = Distance.euclidean

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
      kernSize <- kernelSizeList
    } {
      println(
        s"""
          |Experiment with:
          |  $n points from $sourceName (dimension $dim)
          |  k  = $k
          |  k' = $kernSize
        """.stripMargin)
      val experiment = new Experiment()
        .tag("experiment", "Points")
        .tag("version", BuildInfo.version)
        .tag("git-revision", BuildInfo.gitRevision)
        .tag("git-revcount", BuildInfo.gitRevCount)
        .tag("source", sourceName)
        .tag("space-dimension", dim)
        .tag("k", k)
        .tag("num-points", n)
        .tag("kernel-size", kernSize)
        .tag("computeFarthest", computeFarthest)
        .tag("computeMatching", computeMatching)

      val coreset: Coreset[Point] = algorithm match {

        case "mapreduce" =>
          val parallelism = sc.defaultParallelism
          experiment.tag("parallelism", parallelism)
          val points = sc.objectFile[Point](
            DatasetGenerator.filename(directory, sourceName, dim, n, k),
            parallelism)
          Algorithm.mapReduce(points, kernSize, k, distance, experiment)

        case "streaming" =>
          val points = SerializationUtils.sequenceFile(
            DatasetGenerator.filename(directory, sourceName, dim, n, k))
          Algorithm.streaming(points, k, kernSize, distance, experiment)

        case "sequential" =>
          val points = SerializationUtils.sequenceFile(
            DatasetGenerator.filename(directory, sourceName, dim, n, k))
          Algorithm.sequential(points.toVector, experiment)

        case "random" =>
          val points = SerializationUtils.sequenceFile(
            DatasetGenerator.filename(directory, sourceName, dim, n, k))
          val prob = kernSize.toDouble / n
          Algorithm.random(points, k, prob, distance, experiment)

      }

      Approximation.approximate(
        coreset, k, distance, computeFarthest, computeMatching, 32, experiment)

      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

  }

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val algorithm = opt[String](default = Some("sequential"))

    lazy val source = opt[String](default = Some("versor"))

    lazy val spaceDimension = opt[String](default = Some("2"))

    lazy val delegates = opt[String](required = true)

    lazy val kernelSize = opt[String](required = true)

    lazy val numPoints = opt[String](required = true)

    lazy val runs = opt[Int](default = Some(1))

    lazy val directory = opt[String](default = Some("/tmp"))

    lazy val farthest = toggle(
      default=Some(true),
      descrYes = "Compute metrics based on the farthest-point heuristic",
      descrNo  = "Don't compute metrics based on the farthest-point heuristic")

    lazy val matching = toggle(
      default=Some(true),
      descrYes = "Compute metrics based on the matching heuristic",
      descrNo  = "Don't compute metrics based on the matching heuristic")

  }


}
