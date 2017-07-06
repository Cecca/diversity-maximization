package it.unipd.dei.diversity.matroid

import it.unipd.dei.diversity._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.io.Source
import scala.util.Random

case class WikiPage(id: Long, title: String, categories: Array[String], vector: Vector) {
  override def toString: String = s"($id) `$title` ${categories.mkString("[", ", ", "]")}"
}

object WikiPage {

  private val TWO_OVER_PI: Double = 2.0 / math.Pi
  private val ONE_OVER_PI: Double = 1.0 / math.Pi

  def cosine(aPage: WikiPage, bPage: WikiPage): Double = {
    val a: Vector = aPage.vector
    val b: Vector = bPage.vector
    require(a.numNonzeros > 0, "First vector is zero-valued")
    require(b.numNonzeros > 0, "Second vector is zero-valued")
    require(a.size == b.size)
    var numerator: Double = 0.0
    a.foreachActive { case (i, ca) =>
      numerator += ca * b(i)
    }
    val denomA = Vectors.norm(a, 2)
    val denomB = Vectors.norm(b, 2)
    val res = numerator / (denomA * denomB)
    math.min(res, 1.0)
  }

  def distanceOnlyPositiveComponents(aPage: WikiPage, bPage: WikiPage): Double = {
    PerformanceMetrics.distanceFnCounterInc()
    val cos = cosine(aPage, bPage)
    require(0.0 <= cos && cos <= 1.0, s"Cosine out of range: $cos")
    val dist = TWO_OVER_PI * math.acos(cos)
    require(dist != Double.NaN, "Distance NaN")
    require(dist < Double.PositiveInfinity, "Points at infinite distance!!")
    require(dist > Double.NegativeInfinity, "Points at negative infinite distance!!")
    dist
  }

  def distanceArbitraryComponents(aPage: WikiPage, bPage: WikiPage): Double = {
    PerformanceMetrics.distanceFnCounterInc()
    val cos = cosine(aPage, bPage)
    require(-1.0 <= cos && cos <= 1.0, s"Cosine out of range: $cos")
    val dist = ONE_OVER_PI * math.acos(cos)
    require(dist != Double.NaN, "Distance NaN")
    require(dist < Double.PositiveInfinity, "Points at infinite distance!!")
    require(dist > Double.NegativeInfinity, "Points at negative infinite distance!!")
    dist
  }

}

class WikipediaExperiment(override val spark: SparkSession,
                          val dataPath: String,
                          val categoriesPath: Option[String]) extends ExperimentalSetup[WikiPage] {
  import spark.implicits._

  override val distance: (WikiPage, WikiPage) => Double = WikiPage.distanceArbitraryComponents

  private lazy val rawData: Dataset[WikiPage] = spark.read.parquet(dataPath).as[WikiPage].cache()

  lazy val categories: Array[String] = categoriesPath match {
    case Some(path) =>
      Source.fromFile(path).getLines().toArray
    case None =>
      rawData.select("categories").as[Seq[String]].flatMap(identity).distinct().collect()
  }

  override def loadDataset(): Dataset[WikiPage] = {
    val brCategories = spark.sparkContext.broadcast(categories.toSet)
    rawData.flatMap { wp =>
      val cs = brCategories.value
      val cats = wp.categories.filter(cs.contains)
      if (cats.nonEmpty) {
        Iterator( wp.copy(categories = cats) )
      } else {
        Iterator.empty
      }
    }.mapPartitions(Random.shuffle(_))
      .persist(StorageLevel.MEMORY_ONLY)
  }

  override lazy val matroid: Matroid[WikiPage] =
    new TransversalMatroid[WikiPage, String](categories, _.categories)

}