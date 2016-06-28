package it.unipd.dei.diversity

import it.unipd.dei.experiment.Experiment
import org.apache.spark.storage.StorageLevel
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
    val approxRuns = opts.approxRuns()
    val computeFarthest = opts.farthest()
    val computeMatching = opts.matching()
    val directory = opts.directory()
    val partitioning = opts.partitioning()

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
        .tag("git-branch", BuildInfo.gitBranch)
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
          experiment.tag("partitioning", partitioning)
          val inputPoints = sc.objectFile[Point](
            DatasetGenerator.filename(directory, sourceName, dim, n, k), parallelism)
          val points = partitioning match {
            case "random"  => Partitioning.random(inputPoints, experiment)
            case "polar2D" => Partitioning.polar2D(inputPoints, experiment)
            case "grid"    => Partitioning.grid(inputPoints, experiment)
            case "radius"  => Partitioning.radius(inputPoints, Point.zero(dim), distance, experiment)
            case err       => throw new IllegalArgumentException(s"Unknown partitioning scheme $err")
          }
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
        coreset, k, distance, computeFarthest, computeMatching, approxRuns,
        Some(pointToRow(distance, dim) _), experiment)

      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

  }

  def pointToRow(distance: (Point, Point) => Double, dim: Int)(p: Point)
  : Map[String, Any] = Map(
    "norm" -> distance(p, Point.zero(dim)),
    "point" -> p.toString
  )

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val algorithm = opt[String](default = Some("sequential"))

    lazy val source = opt[String](default = Some("versor"))

    lazy val partitioning = opt[String](default = Some("random"))

    lazy val spaceDimension = opt[String](default = Some("2"))

    lazy val delegates = opt[String](required = true)

    lazy val kernelSize = opt[String](required = true)

    lazy val numPoints = opt[String](required = true)

    lazy val runs = opt[Int](default = Some(1))

    lazy val approxRuns = opt[Int](default = Some(1))

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
