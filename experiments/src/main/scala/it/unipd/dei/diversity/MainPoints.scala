package it.unipd.dei.diversity

import it.unipd.dei.experiment.Experiment
import org.apache.spark.{SparkConf, SparkContext}

object MainPoints {

  def main(args: Array[String]) {

    // Read command line options
    val opts = new PointsExperimentConf(args)
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
      val experiment = new Experiment()
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
        coreset, k, distance, computeFarthest, computeMatching, experiment)

      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

  }

}
