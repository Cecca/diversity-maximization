package it.unipd.dei.diversity

import it.unipd.dei.diversity.matroid.{Matroid, TransversalMatroid, WikiPage}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.ScallopConf

import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Random

object Diameter {

  private def farthestFrom[T:ClassTag](points: RDD[T],
                                       x: T,
                                       threshold: Double,
                                       matroid: Matroid[T],
                                       distance: (T, T) => Double): Double = {
    points.flatMap({y =>
      val d = distance(x, y)
      if (d >= threshold && matroid.isIndependent(Seq(x, y))) {
        Iterator(d)
      } else {
        Iterator.empty
      }
    }).max()
  }

  def diameterLowerBound[T:ClassTag](points: RDD[T],
                                     matroid: Matroid[T],
                                     distance: (T, T) => Double)(implicit ordering: Ordering[T]): Double = {
    val combinations = points
      .cartesian(points)
      .filter({case (x, y) => ordering.compare(x, y) < 0})
      .map({case k@(x, y) => (distance(x, y), k)})
    val (d, (x1, x2)) = combinations.max()(Ordering.by(_._1))
    if (matroid.isIndependent(Array(x1, x2))) {
      println("The farthest points form an independent set")
      return d
    }
    println("We have to seek another point for the diameter estimate")
    val threshold = d/2
    val dFromX1 = farthestFrom(points, x1, threshold, matroid, distance)
    val dFromX2 = farthestFrom(points, x2, threshold, matroid, distance)
    math.max(dFromX1, dFromX2)
  }

  def main(args: Array[String]) {
    val opts = new Options(args)
    opts.verify()

    val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("Matroid diversity")
    val spark = SparkSession.builder()
      .config(sparkConfig)
      .getOrCreate()

    val distance: (WikiPage, WikiPage) => Double = WikiPage.distanceArbitraryComponents

    import spark.implicits._
    implicit val ord: Ordering[WikiPage] = Ordering.by(page => page.id)

    val dataset = spark.read.parquet(opts.input()).as[WikiPage].cache()
    val categories =
      if (opts.categories.isDefined) {
        val _cats = Source.fromFile(opts.categories()).getLines().toArray
        _cats
      } else {
        val _cats = dataset.select("categories").as[Seq[String]].flatMap(identity).distinct().collect()
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
    }.mapPartitions(Random.shuffle(_)).rdd.persist(StorageLevel.MEMORY_ONLY)

    val lowerBound = diameterLowerBound(filteredDataset, matroid, distance)
    println(s"Diameter lower bound is $lowerBound")
  }

  private class Options(args: Array[String]) extends ScallopConf(args) {

    lazy val categories = opt[String](required = false, argName = "FILE")

    lazy val input = opt[String](required = true)

  }

}
