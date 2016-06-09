package it.unipd.dei.diversity

import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.experiment.Experiment
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

object Algorithm {

  def streaming[T:ClassTag](points: Iterator[T],
                            k: Int,
                            kernelSize: Int,
                            distance: (T, T) => Double,
                            experiment: Experiment): StreamingCoreset[T] = {
    experiment.tag("algorithm", "Streaming")
    println("Run streaming algorithm!")
    val coreset = new StreamingCoreset[T](kernelSize, k, distance)
    val (_, coresetTime) = timed {
      for (p <- points) {
        coreset.update(p)
      }
    }
    val updatesTimer = coreset.updatesTimer.getSnapshot
    experiment.append("times",
      jMap(
        "component" -> "algorithm",
        "time"      -> convertDuration(coresetTime, reportTimeUnit)
      ))
    experiment.append("performance",
      jMap(
        "throughput"    -> coreset.updatesTimer.getMeanRate,
        "update-mean"   -> convertDuration(updatesTimer.getMean, reportTimeUnit),
        "update-stddev" -> convertDuration(updatesTimer.getStdDev, reportTimeUnit),
        "update-max"    -> convertDuration(updatesTimer.getMax, reportTimeUnit),
        "update-min"    -> convertDuration(updatesTimer.getMin, reportTimeUnit),
        "update-median" -> convertDuration(updatesTimer.getMedian, reportTimeUnit)
      ))
    coreset
  }

  def mapReduce[T:ClassTag](points: RDD[T],
                            kernelSize: Int,
                            k: Int,
                            distance: (T, T) => Double,
                            experiment: Experiment): MapReduceCoreset[T] = {
    require(kernelSize >= k)
    experiment.tag("algorithm", "MapReduce")

    val parallelism = points.sparkContext.defaultParallelism
    // We distinguish the case of increasing or decreasing the number of
    // partitions for efficiency
    val repartitioned =
      if (points.getNumPartitions < parallelism) {
        points.repartition(parallelism)
      } else if (points.getNumPartitions > parallelism) {
        points.coalesce(parallelism)
      } else {
        points
      }

    println("Run MapReduce algorithm!")
    val partitionCnt = points.sparkContext.accumulator(0L, "partition counter")
    val (coreset, mrTime) = timed {
      repartitioned.mapPartitions { pts =>
        partitionCnt += 1
        val pointsArr: Array[T] = pts.toArray
        val coreset = MapReduceCoreset.run(
          pointsArr,
          kernelSize,
          k,
          distance)
        Iterator(coreset)
      }.reduce { (a, b) =>
        MapReduceCoreset.compose(a, b)
      }
    }
    require(partitionCnt.value == parallelism,
      s"Processed ${partitionCnt.value} partitions")

    experiment.append("times",
      jMap(
        "component" -> "algorithm",
        "time"      -> convertDuration(mrTime, reportTimeUnit)
      ))

    coreset
  }

  /**
    * Doesn't do much, just wraps the entire input in a coreset, just
    * for uniformity with the rest.
    */
  def sequential[T:ClassTag](inputPoints: Vector[T],
                             experiment: Experiment): Coreset[T] = {
    experiment.tag("algorithm", "Sequential")
    experiment.append("times",
      jMap(
        "component" -> "algorithm",
        "time"      -> 0.0
      ))
    new Coreset[T] {
      override def kernel: Vector[T] = inputPoints
      override def delegates: Vector[T] = Vector.empty
    }
  }

  def random[T:ClassTag](input: Iterator[T],
                         k: Int,
                         sampleProb: Double,
                         distance: (T, T) => Double,
                         experiment: Experiment): Coreset[T] = {
    experiment.tag("algorithm", "Random")
    println("Run random algorithm!")
    val sample = ArrayBuffer[T]()
    val (_, time) = timed {
      for (p <- input) {
        if (Random.nextDouble() <= sampleProb) {
          sample.append(p)
        }
      }
    }
    val randomSubset = Random.shuffle(sample).take(k).toVector
    require(randomSubset.size == k)

    experiment.append("times",
      jMap(
        "component" -> "algorithm",
        "time"      -> convertDuration(time, reportTimeUnit)
      ))

    new Coreset[T] {
      override def kernel: Vector[T] = randomSubset
      override def delegates: Vector[T] = Vector.empty
    }
  }


}
