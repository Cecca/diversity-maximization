package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import it.unimi.dsi.logging.ProgressLogger
import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.diversity.source.PointSource
import it.unipd.dei.experiment.Experiment
import org.rogach.scallop.ScallopConf

object MainStreaming {

  def run(source: PointSource,
          kernelSize: Int,
          computeFarthest: Boolean,
          computeMatching: Boolean,
          experiment: Experiment) = {
    val coreset = new StreamingCoreset(kernelSize, source.k, source.distance)
    val sourceIt = source.iterator

    val (_, coresetTime) = timed {
      while (sourceIt.hasNext) {
        coreset.update(sourceIt.next())
      }
    }

    val points = coreset.points.toArray

    val (farthestSubset, farthestSubsetTime): (Option[IndexedSeq[Point]], Long) =
      if (computeFarthest) {
        timed {
          Some(FarthestPointHeuristic.run(points, source.k, source.distance))
        }
      } else {
        (None, 0)
      }

    val (matchingSubset, matchingSubsetTime): (Option[IndexedSeq[Point]], Long) =
      if (computeMatching) {
        timed {
          Some(MatchingHeuristic.run(points, source.k, source.distance))
        }
      } else {
        (None, 0)
      }

    computeApproximations(source, farthestSubset, matchingSubset).foreach { row =>
      experiment.append("approximation", row)
    }

    val updatesTimer = coreset.updatesTimer.getSnapshot
    val reportTimeUnit = TimeUnit.MILLISECONDS
    experiment.append("performance",
      jMap(
        "throughput"    -> coreset.updatesTimer.getMeanRate,
        "coreset-time"  -> convertDuration(coresetTime, reportTimeUnit),
        "farthest-time" -> convertDuration(farthestSubsetTime, reportTimeUnit),
        "matching-time" -> convertDuration(matchingSubsetTime, reportTimeUnit),
        "update-mean"   -> convertDuration(updatesTimer.getMean, reportTimeUnit),
        "update-stddev" -> convertDuration(updatesTimer.getStdDev, reportTimeUnit),
        "update-max"    -> convertDuration(updatesTimer.getMax, reportTimeUnit),
        "update-min"    -> convertDuration(updatesTimer.getMin, reportTimeUnit),
        "update-median" -> convertDuration(updatesTimer.getMedian, reportTimeUnit)
      ))
  }

  def main(args: Array[String]) {
    val opts = new PointsExperimentConf(args)
    opts.verify()

    val sourcesList = opts.source().split(",")
    val dimList = opts.spaceDimension().split(",").map{_.toInt}
    val kList = opts.delegates().split(",").map{_.toInt}
    val numPointsList = opts.numPoints().split(",").map{_.toInt}
    val kernelSizeList = opts.kernelSize().split(",").map{_.toInt}
    val runs = opts.runs()
    val materialize = opts.materialize()
    val computeFarthest = opts.farthest()
    val computeMatching = opts.matching()

    val pl = new ProgressLogger("experiments")
    pl.displayFreeMemory = true
    pl.expectedUpdates =
      dimList.length*kList.length*numPointsList.length*kernelSizeList.length
    pl.start(s"Starting ${pl.expectedUpdates} experiments")
    for {
      r <- 0 until runs
      sourceName   <- sourcesList
      dim      <- dimList
      k        <- kList
      n        <- numPointsList
      kernSize <- kernelSizeList
    } {
      val experiment = new Experiment()
        .tag("version", BuildInfo.version)
        .tag("git-revision", BuildInfo.gitRevision)
        .tag("git-revcount", BuildInfo.gitRevCount)
        .tag("source", sourceName)
        .tag("space-dimension", dim)
        .tag("k", k)
        .tag("num-points", n)
        .tag("kernel-size", kernSize)
        .tag("algorithm", "Streaming")
        .tag("materialize", materialize)
        .tag("computeFarthest", computeFarthest)
        .tag("computeMatching", computeMatching)
      val source =
        if (materialize) {
          PointSource(sourceName, dim, n, k, Distance.euclidean).materialize()
        } else {
          PointSource(sourceName, dim, n, k, Distance.euclidean)
        }
      run(source, kernSize, computeFarthest, computeMatching, experiment)
      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)

      pl.update()
    }
    pl.stop("Done")
  }

}
