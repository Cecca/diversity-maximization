package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import it.unipd.dei.experiment.Experiment
import ExperimentUtil._

object Algorithm {

  def streaming[T](points: Iterator[T],
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
    val reportTimeUnit = TimeUnit.MILLISECONDS
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

}
