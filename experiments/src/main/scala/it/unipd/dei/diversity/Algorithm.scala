package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.experiment.Experiment
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Algorithm {

  val reportTimeUnit = TimeUnit.MILLISECONDS

  def streaming[T:ClassTag](points: Iterator[T],
                            k: Int,
                            kernelSize: Int,
                            distance: (T, T) => Double,
                            experiment: Experiment): StreamingCoreset[T] = {
    experiment.tag("algorithm", "Streaming")
    val coreset = new StreamingCoreset[T](kernelSize, k, distance)
    val (_, coresetTime) = timed {
      for (p <- points) {
        coreset.update(p)
      }
    }
    val updatesTimer = coreset.updatesTimer.getSnapshot
    experiment.append("performance",
      jMap(
        "throughput"    -> coreset.updatesTimer.getMeanRate,
        "coreset-time"  -> convertDuration(coresetTime, reportTimeUnit),
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

    val parallelism = points.sparkContext.defaultParallelism

    println("Run!!")
    val partitionCnt = points.sparkContext.accumulator(0L, "partition counter")
    val (coreset, mrTime) = timed {
      points.coalesce(parallelism).mapPartitions { pts =>
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

    experiment.append("performance",
      jMap(
        "component" -> "MapReduce",
        "time" -> convertDuration(mrTime, reportTimeUnit)
      ))

    coreset
  }


}
