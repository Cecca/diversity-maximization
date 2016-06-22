package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.{Algorithm, Approximation, SerializationUtils, _}
import it.unipd.dei.experiment.Experiment
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object MainWiki {

  def wikiBowToMap(bow: WikiBagOfWords) = Map(
    "title" -> bow.title,
    "categories" -> bow.categories.mkString(",")
  )

  def main(args: Array[String]) {

    // Read command line options
    val opts = new Conf(args)
    opts.verify()
    val dataset = opts.dataset()
    val kList = opts.delegates().split(",").map{_.toInt}
    val kernelSizeList = opts.kernelSize().split(",").map{_.toInt}
    val runs = opts.runs()
    val approxRuns = opts.approxRuns()
    val computeFarthest = opts.farthest()
    val computeMatching = opts.matching()
    val queryTitle = opts.queryTitle.get
    val queryRadius = opts.queryRadius()

    val distance: (WikiBagOfWords, WikiBagOfWords) => Double = WikiBagOfWords.cosineDistance

    val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("Wikipedia MapReduce coresets")
    val sc = new SparkContext(sparkConfig)

    for {
      r        <- 0 until runs
      k        <- kList
      kernSize <- kernelSizeList
    } {
      println(
        s"""
           |Experiment on $dataset
           |  k  = $k
           |  k' = $kernSize
        """.stripMargin)
      val experiment = new Experiment()
        .tag("experiment", "Wikipedia")
        .tag("version", BuildInfo.version)
        .tag("git-revision", BuildInfo.gitRevision)
        .tag("git-revcount", BuildInfo.gitRevCount)
        .tag("git-branch", BuildInfo.gitBranch)
        .tag("k", k)
        .tag("kernel-size", kernSize)
        .tag("computeFarthest", computeFarthest)
        .tag("computeMatching", computeMatching)

      val parallelism = sc.defaultParallelism
      experiment.tag("parallelism", parallelism)
      val documents = CachedDataset(sc, dataset).persist(StorageLevel.MEMORY_AND_DISK)
      val filteredDocuments = queryTitle match {
        case Some(title) =>
          experiment.tag("query-title", queryTitle)
          experiment.tag("query-radius", queryRadius)
          SubsetSelector.selectSubset(documents, title, distance, queryRadius)
            .persist(StorageLevel.MEMORY_AND_DISK)
        case None => documents
      }
      println(s"Working on ${filteredDocuments.count()} documents over ${documents.count()}")
      documents.unpersist()
      val coreset: Coreset[WikiBagOfWords] =
        Algorithm.mapReduce(filteredDocuments, kernSize, k, distance, experiment)

      // Display coreset on console
      println(coreset.points.map(bow => s"${bow.title} :: ${bow.categories}").mkString("\n"))

      Approximation.approximate(
        coreset, k, distance, computeFarthest, computeMatching,
        approxRuns, Some(wikiBowToMap _), experiment)

      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

  }

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val dataset = opt[String](required = true)

    lazy val delegates = opt[String](required = true)

    lazy val kernelSize = opt[String](required = true)

    lazy val runs = opt[Int](default = Some(1))

    lazy val approxRuns = opt[Int](default = Some(1))

    lazy val queryTitle = opt[String]()

    lazy val queryRadius = opt[Double](default=Some(1.0))

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
