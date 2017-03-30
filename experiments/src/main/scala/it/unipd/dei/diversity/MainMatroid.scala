package it.unipd.dei.diversity

import it.unipd.dei.diversity.matroid.TransversalMatroid
import it.unipd.dei.diversity.wiki.WikiPage
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object MainMatroid {

  // Set up Spark lazily, it will be initialized only if the algorithm needs it.
  lazy val spark = {
    lazy val sparkConfig = new SparkConf(loadDefaults = true)
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

    val distance: (WikiPage, WikiPage) => Double = WikiPage.distanceArbitraryComponents

    import spark.implicits._
    val dataset = spark.read.parquet(opts.input()).as[WikiPage].cache()
    val categories = dataset.select("categories").as[Seq[String]].flatMap(identity).distinct().collect()
    val matroid = new TransversalMatroid[WikiPage, String](categories, _.categories)

    opts.algorithm() match {
      case "local-search" =>
        val localDataset = dataset.collect()
        dataset.unpersist(blocking = true)
        val solution = LocalSearch.remoteClique(
          localDataset, opts.k(), opts.gamma(), matroid, distance)

        println(solution)
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

  }

}
