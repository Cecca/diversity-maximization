package it.unipd.dei.diversity.matroid

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag

abstract class ExperimentalSetup[T:ClassTag] {
  val spark: SparkSession
  val distance: (T, T) => Double
  val matroid: Matroid[T]

  def pointToMap(point: T): Map[String, Any]

  def loadDataset(): Dataset[T]

  def loadLocally(): Array[T] = {
    loadDataset().rdd.collect()
    // val data = loadDataset()
    // val cnt = data.count
    // require(cnt < Int.MaxValue.toLong)
    // println(s"Trying to allocate $cnt elements")
    // val localDataset: Array[T] = Array.ofDim[T](cnt.toInt)
    // println("Allocation successful")
    // val dataIt = data.toLocalIterator
    // var i = 0
    // while (dataIt.hasNext) {
    //   localDataset(i) = dataIt.next()
    //   i += 1
    // }
    // println("Collected data locally")
    // localDataset
  }

}
