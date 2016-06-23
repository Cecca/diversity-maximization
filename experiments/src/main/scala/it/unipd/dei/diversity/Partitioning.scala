package it.unipd.dei.diversity

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

/**
  * Various partitioning strategies. Some strategies can be
  * applied only to certain data types.
  */
object Partitioning {

  def random[T](rdd: RDD[T]): RDD[T] = {
    val parallelism = rdd.sparkContext.defaultParallelism
    // We distinguish the case of increasing or decreasing the number of
    // partitions for efficiency
    if (rdd.getNumPartitions < parallelism) {
      println("Increasing the number of partitions")
      rdd.repartition(parallelism)
    } else if (rdd.getNumPartitions > parallelism) {
      println("Decreasing the number of partitions")
      rdd.coalesce(parallelism)
    } else {
      rdd
    }
  }

  def radius(rdd: RDD[Point],
             zero: Point,
             distance: (Point, Point) => Double): RDD[Point] = {
    val parallelism = rdd.sparkContext.defaultParallelism
    rdd.map { p =>
      val pidx = math.floor(distance(p, zero)*parallelism).toInt
      (pidx, p)
    }.partitionBy(new HashPartitioner(parallelism)).mapPartitions(
      { points => points.map(_._2) },
      preservesPartitioning = true)
  }

  def polar2D(rdd: RDD[Point]): RDD[Point] = {
    val parallelism = rdd.sparkContext.defaultParallelism
    rdd.map { p =>
      require(p.dimension >= 2)
      val x = p(0)
      val y = p(1)
      val angle = math.atan2(y, x)
      val shiftedAngle = if (angle >= 0) angle else angle + 2*math.Pi
      val pidx = math.floor(shiftedAngle/(2*math.Pi) * parallelism).toInt
      (pidx, p)
    }.partitionBy(new HashPartitioner(parallelism)).mapPartitions(
      { points => points.map(_._2) },
      preservesPartitioning = true)
  }
}
