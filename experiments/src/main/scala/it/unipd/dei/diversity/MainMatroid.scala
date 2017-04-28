package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import it.unipd.dei.diversity.ExperimentUtil.{jMap, timed}
import it.unipd.dei.diversity.matroid.TransversalMatroid
import it.unipd.dei.diversity.wiki.WikiPage
import it.unipd.dei.experiment.Experiment
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scala.io.Source
import scala.util.Random

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

  def cliqueDiversity[T](subset: IndexedSubset[T],
                         distance: (T, T) => Double): Double = {
    val n = subset.superSet.length
    var currentDiversity: Double = 0
    var i = 0
    while (i<n) {
      if (subset.contains(i)) {
        var j = i + 1
        while (j < n) {
          if (subset.contains(j)) {
            currentDiversity += distance(subset.get(i).get, subset.get(j).get)
          }
          j += 1
        }
      }
      i += 1
    }
    currentDiversity
  }

  private def collectLocally(data: Dataset[WikiPage], numElements: Long): Array[WikiPage] = {
    val dataIt = data.toLocalIterator()
    val localDataset: Array[WikiPage] = Array.ofDim[WikiPage](numElements.toInt)
    var i = 0
    while (dataIt.hasNext) {
      localDataset(i) = dataIt.next()
      i += 1
    }
    println("Collected dataset locally")
    data.unpersist(blocking = true)
    localDataset
  }

  def main(args: Array[String]) {
    val opts = new Opts(args)
    opts.verify()

    val experiment = new Experiment()
      .tag("input", opts.input())
      .tag("k", opts.k())
      .tag("gamma", opts.gamma())
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
    }.mapPartitions(Random.shuffle(_)).cache()
    val numElements = filteredDataset.count()
    println(s"The filtered dataset has $numElements elements")
    dataset.unpersist(blocking = true)
    experiment.tag("filtered dataset size", numElements)


    opts.algorithm() match {
      case "local-search" =>
        val localDataset: Array[WikiPage] = collectLocally(filteredDataset, numElements)
        val (solution, t) = timed {
          LocalSearch.remoteClique[WikiPage](
            localDataset, opts.k(), opts.gamma(), matroid, distance)
        }

        experiment.append("performance",
          jMap(
            "diversity" -> Diversity.clique(solution, distance),
            "time" -> ExperimentUtil.convertDuration(t, TimeUnit.MILLISECONDS)))

        for (wp <- solution) {
          experiment.append("solution",
            jMap(
              "title" -> wp.title,
              "categories" -> wp.categories))
        }

      case "sequential-coreset" =>

    }

    println(experiment.toSimpleString)
    experiment.saveAsJsonFile(true)

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
