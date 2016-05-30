package it.unipd.dei.diversity

import org.apache.spark.{Logging, SparkConf, SparkContext}

object LocalSparkContext extends Logging {

  def withSpark[T](f: SparkContext => T): T = {
    val cores = Runtime.getRuntime().availableProcessors()
    val par = 2 * cores
    val conf = new SparkConf().set("spark.default.parallelism", par.toString)
    val sc = new SparkContext("local", "test", conf)
    logInfo(s"Spark parallelism ${sc.defaultParallelism}")
    try {
      f(sc)
    } finally {
      sc.stop()
    }
  }

}