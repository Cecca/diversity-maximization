package it.unipd.dei.diversity

import it.unipd.dei.diversity.source.PointSource
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import scala.util.Random

object DatasetGenerator {

  def filename(dir: String, sourceName: String, dim: Int, n: Int, k: Int) =
    s"$dir/$sourceName-$dim-$n-$k.points"

  def main(args: Array[String]) {

    val opts = new Conf(args)
    opts.verify()

    val sourcesList = opts.source().split(",")
    val dimList = opts.spaceDimension().split(",").map{_.toInt}
    val kList = opts.k().split(",").map{_.toInt}
    val numPointsList = opts.numPoints().split(",").map{_.toInt}
    val outputDir = opts.outputDir()

    val randomGen = new Random()

    val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("MapReduce coresets")
    val sc = new SparkContext(sparkConfig)
    for {
      sourceName   <- sourcesList
      dim      <- dimList
      k        <- kList
      n        <- numPointsList
    } {
      val source = PointSource(sourceName, dim, n, k, Distance.euclidean, randomGen)
      
      val rdd = sc.parallelize(source.toVector)
        .persist(StorageLevel.MEMORY_AND_DISK)

      val numGenerated = rdd.count()
      require(numGenerated >= n,
        s"Not enough points have been generated! $numGenerated < $n")
      println(s"Generated $numGenerated points")

      rdd.saveAsObjectFile(filename(outputDir, sourceName, dim, n, k))

    }

  }

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val source = opt[String](default = Some("versor"))

    lazy val spaceDimension = opt[String](default = Some("2"))

    lazy val k = opt[String](required = true)

    lazy val numPoints = opt[String](required = true)

    lazy val outputDir = opt[String](required = true)

  }


}
