// diversity-maximization: Diversity maximization in Streaming and MapReduce
// Copyright (C) 2016  Matteo Ceccarello <ceccarel@dei.unipd.it>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package it.unipd.dei.diversity

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.experiment.Experiment
import org.apache.spark.storage.StorageLevel
import scala.reflect.ClassTag

import scala.util.Random

/**
  * Various partitioning strategies. Some strategies can be
  * applied only to certain data types.
  */
object Partitioning {

  def random(rdd: RDD[Point], experiment: Experiment): RDD[Point] = {
    val (result, time): (RDD[Point], Long) = timed {
      val parallelism = rdd.sparkContext.defaultParallelism
      val _res =
        if (rdd.getNumPartitions < parallelism) {
          println("Increasing the number of partitions")
          rdd.repartition(parallelism)
        } else if (rdd.getNumPartitions > parallelism) {
          println("Decreasing the number of partitions")
          rdd.coalesce(parallelism)
        } else {
          rdd
        }.persist(StorageLevel.MEMORY_AND_DISK)
      // Force count to compute time
      _res.count()
      _res
    }
    experiment.append("times",
      jMap(
        "component" -> "partitioning",
        "time"      -> convertDuration(time, reportTimeUnit)
      ))
    result
  }

  def shuffle[T:ClassTag](rdd: RDD[T], experiment: Experiment): RDD[T] = {
    println("Shuffling the data!")
    val (result, time): (RDD[T], Long) = timed {
      val parallelism = rdd.sparkContext.defaultParallelism
      val _res = rdd.map { p =>
        val pidx = Random.nextInt(parallelism)
        (pidx, p)
      }.partitionBy(new HashPartitioner(parallelism)).mapPartitions({ points => points.map(_._2) }, preservesPartitioning = true)
        .persist(StorageLevel.MEMORY_AND_DISK)
      // Force count to compute time
      _res.count()
      _res
    }
    experiment.append("times",
      jMap(
        "component" -> "partitioning",
        "time"      -> convertDuration(time, reportTimeUnit)
      ))
    result
  }

  def radius(rdd: RDD[Point],
             zero: Point,
             distance: (Point, Point) => Double,
             experiment: Experiment): RDD[Point] = {
    val parallelism = rdd.sparkContext.defaultParallelism
    val (result, time): (RDD[Point], Long) = timed {
      val _res = rdd.map { p =>
        val pidx = math.floor(distance(p, zero)*parallelism).toInt
        (pidx, p)
      }.partitionBy(new HashPartitioner(parallelism))
        .mapPartitions({ points => points.map(_._2) }, preservesPartitioning = true)
        .persist(StorageLevel.MEMORY_AND_DISK)
      // Force evaluation to get timing
      _res.count()
      _res
    }
    experiment.append("times",
      jMap(
        "component" -> "partitioning",
        "time"      -> convertDuration(time, reportTimeUnit)
      ))
    result
  }

  def radiusOld(rdd: RDD[Point],
                zero: Point,
                distance: (Point, Point) => Double,
                experiment: Experiment): RDD[Point] = {
    val parallelism = rdd.sparkContext.defaultParallelism
    val (result, time): (RDD[Point], Long) = timed {
      val _res = rdd.map { p =>
        val pidx = math.floor(distance(p, zero) / 0.8 * parallelism).toInt
        (pidx, p)
      }.partitionBy(new HashPartitioner(parallelism))
        .mapPartitions({ points => points.map(_._2) }, preservesPartitioning = true)
        .persist(StorageLevel.MEMORY_AND_DISK)
      // Force evaluation to get timing
      _res.count()
      _res
    }
    experiment.append("times",
      jMap(
        "component" -> "partitioning",
        "time"      -> convertDuration(time, reportTimeUnit)
      ))
    result
  }

  def polar2D(rdd: RDD[Point], experiment: Experiment): RDD[Point] = {
    val parallelism = rdd.sparkContext.defaultParallelism
    val (res, time) = timed {
      val _res = rdd.map { p =>
        require(p.dimension >= 2)
        val x = p(0)
        val y = p(1)
        val angle = math.atan2(y, x)
        val shiftedAngle = if (angle >= 0) angle else angle + 2 * math.Pi
        val pidx = math.floor(shiftedAngle / (2 * math.Pi) * parallelism).toInt
        (pidx, p)
      }.partitionBy(new HashPartitioner(parallelism)).mapPartitions(
        { points => points.map(_._2) },
        preservesPartitioning = true).persist(StorageLevel.MEMORY_AND_DISK)
      _res.count()
      _res
    }
    experiment.append("times",
      jMap(
        "component" -> "partitioning",
        "time"      -> convertDuration(time, reportTimeUnit)
      ))

    res
  }

  def grid(rdd: RDD[Point], experiment: Experiment): RDD[Point] = {
    val parallelism = rdd.sparkContext.defaultParallelism
    val (res, time) = timed {
      val _res = rdd.map { p =>
        var index: Int = 0
        var multiplier = 1
        for (coord <- p.data) {
          require(coord <= 1 && coord >= -1)
          val j = ((coord + 1.0)*parallelism/2.0).toInt
          require(0 <= j && j <= parallelism)
          index += j*multiplier
          multiplier *= parallelism
        }
        (index, p)
      }.partitionBy(new HashPartitioner(parallelism)).mapPartitions(
        { points => points.map(_._2) },
        preservesPartitioning = true).persist(StorageLevel.MEMORY_AND_DISK)
      _res.count()
      _res
    }
    experiment.append("times",
      jMap(
        "component" -> "partitioning",
        "time"      -> convertDuration(time, reportTimeUnit)
      ))
    res
  }

  def unitGrid(rdd: RDD[Point], experiment: Experiment): RDD[Point] = {
    val parallelism = rdd.sparkContext.defaultParallelism
    val (res, time) = timed {
      val _res = rdd.map { p =>
        var index: Int = 0
        var multiplier = 1
        for (coord <- p.data) {
          require(coord <= 1 && coord >= 0)
          val j = (coord*parallelism).toInt
          require(0 <= j && j <= parallelism)
          index += j*multiplier
          multiplier *= parallelism
        }
        (index, p)
      }.partitionBy(new HashPartitioner(parallelism)).mapPartitions(
        { points => points.map(_._2) },
        preservesPartitioning = true).persist(StorageLevel.MEMORY_AND_DISK)
      _res.count()
      _res
    }
    experiment.append("times",
      jMap(
        "component" -> "partitioning",
        "time"      -> convertDuration(time, reportTimeUnit)
      ))
    res
  }

}
