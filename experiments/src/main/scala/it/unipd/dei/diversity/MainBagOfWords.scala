package it.unipd.dei.diversity

import it.unipd.dei.diversity.words.{BagOfWordsDataset, UCIBagOfWords}
import it.unipd.dei.experiment.Experiment
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object MainBagOfWords {

  def main(args: Array[String]) {

    // Read command line options
    val opts = new Conf(args)
    opts.verify()
    val algorithm = opts.algorithm()
    val kList = opts.delegates().split(",").map{_.toInt}
    val kernelSizeList = opts.kernelSize().split(",").map{_.toInt}
    val runs = opts.runs()
    val computeFarthest = opts.farthest()
    val computeMatching = opts.matching()
    val datasets = opts.dataset().split(",")
    val directory = opts.directory()

    val distance: (UCIBagOfWords, UCIBagOfWords) => Double = Distance.jaccard[Int]

    // Set up Spark lazily, it will be initialized only if the algorithm needs it.
    lazy val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("MapReduce coresets")
    lazy val sc = new SparkContext(sparkConfig)

    // Cycle through parameter configurations
    for {
      r        <- 0 until runs
      dataset  <- datasets
      k        <- kList
      kernSize <- kernelSizeList
    } {
      val experiment = new Experiment()
        .tag("experiment", "BagOfWords")
        .tag("version", BuildInfo.version)
        .tag("git-revision", BuildInfo.gitRevision)
        .tag("git-revcount", BuildInfo.gitRevCount)
        .tag("k", k)
        .tag("kernel-size", kernSize)
        .tag("computeFarthest", computeFarthest)
        .tag("computeMatching", computeMatching)

      val data = BagOfWordsDataset.fromName(dataset, directory)

      val coreset: Coreset[UCIBagOfWords] = algorithm match {

        case "mapreduce" =>
          val parallelism = sc.defaultParallelism
          experiment.tag("parallelism", parallelism)
          val input = data.documents(sc)
          Algorithm.mapReduce(input, kernSize, k, distance, experiment)

        case "streaming" =>
          ???

        case "sequential" =>
          ???

        case "random" =>
          ???

      }

      Approximation.approximate[UCIBagOfWords](
        coreset, k, distance, computeFarthest, computeMatching, experiment)

      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

  }

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val algorithm = opt[String](default = Some("sequential"))

    lazy val dataset = opt[String](required = true)

    lazy val directory = opt[String](required = true)

    lazy val delegates = opt[String](required = true)

    lazy val kernelSize = opt[String](required = true)

    lazy val runs = opt[Int](default = Some(1))

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
