package it.unipd.dei.diversity

import it.unipd.dei.diversity.ExperimentUtil.{jMap, timed}
import it.unipd.dei.diversity.matroid.TransversalMatroid
import it.unipd.dei.diversity.wiki.WikiPage
import it.unipd.dei.experiment.Experiment
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scala.io.Source

object MainMatroid {

  // Set up Spark lazily, it will be initialized only if the algorithm needs it.
  lazy val spark = {
    val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("Matroid diversity")
    val _s = SparkSession.builder()
      .config(sparkConfig)
      .getOrCreate()
    _s.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    _s
  }

  def main(args: Array[String]) {
    val opts = new Opts(args)
    opts.verify()

    val experiment = new Experiment()
      .tag("input", opts.input())
      .tag("k", opts.k())
      .tag("algorithm", opts.algorithm())
      .tag("version", BuildInfo.version)
      .tag("git-revision", BuildInfo.gitRevision)
      .tag("git-revcount", BuildInfo.gitRevCount)
      .tag("git-branch", BuildInfo.gitBranch)
    for ((k, v) <- SerializationUtils.metadata(opts.input())) {
      experiment.tag("input." + k, v)
    }

    val distance: (WikiPage, WikiPage) => Double = WikiPage.distanceArbitraryComponents

    import spark.implicits._
    val dataset = spark.read.parquet(opts.input()).as[WikiPage].cache()
    val categories =
      if (opts.categories.isDefined) {
        val _cats = Source.fromFile(opts.categories()).getLines().toArray
        experiment.tag("query-categories", _cats)
        _cats
      } else {
        val _cats = dataset.select("categories").as[Seq[String]].flatMap(identity).distinct().collect()
        experiment.tag("query-categories", "*all*")
        _cats
      }
    val matroid = new TransversalMatroid[WikiPage, String](categories, _.categories)

    val brCategories = spark.sparkContext.broadcast(categories.toSet)
    val filteredDataset = dataset.flatMap { wp =>
      val cs = brCategories.value
      val cats = wp.categories.filter(cs.contains)
      if (cats.nonEmpty) {
        Iterator( wp.copy(categories = cats) )
      } else {
        Iterator.empty
      }
    }.cache()
    val numElements = filteredDataset.count()
    println(s"The filtered dataset has $numElements elements")
    dataset.unpersist(blocking = true)


    opts.algorithm() match {
      case "local-search" =>
        val dataIt = filteredDataset.toLocalIterator()
        val localDataset = Array.ofDim[WikiPage](numElements.toInt)
        var i = 0
        while (dataIt.hasNext) {
          localDataset(i) = dataIt.next()
          i += 1
        }
        println("Collected dataset locally")
        dataset.unpersist(blocking = true)
        val (solution, t) = timed {
          LocalSearch.remoteClique(
            localDataset, opts.k(), opts.gamma(), matroid, distance)
        }

        experiment.append("performance",
          jMap("time" -> t))

        for (wp <- solution) {
          experiment.append("solution",
            jMap(
              "title" -> wp.title,
              "categories" -> wp.categories))
        }

    }

  }

  class Opts(args: Array[String]) extends ScallopConf(args) {

    lazy val algorithm = opt[String](default = Some("local-search"))

    lazy val k = opt[Int](required = true)

    lazy val kernelSize = opt[String](required = false)

    lazy val gamma = opt[Double](default = Some(0.0))

    // TODO Use this option
    lazy val approxRuns = opt[Int](default = Some(1))

    lazy val input = opt[String](required = true)

    lazy val categories = opt[String](required = false, argName = "FILE")

  }

}
