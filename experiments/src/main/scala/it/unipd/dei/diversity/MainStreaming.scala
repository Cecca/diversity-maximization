package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import it.unimi.dsi.logging.ProgressLogger
import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.diversity.source.PointSource
import it.unipd.dei.experiment.Experiment
import org.rogach.scallop.ScallopConf

object MainStreaming {

  def timed[T](fn: => T): (T, Long) = {
    val start = System.nanoTime()
    val res = fn
    val end = System.nanoTime()
    (res, end - start)
  }

  def convertDuration(duration: Double, unit: TimeUnit): Double =
    duration / unit.toNanos(1)

  def run(source: PointSource, kernelSize: Int, experiment: Experiment) = {
    val coreset = new StreamingCoreset(kernelSize, source.k, source.distance)
    val sourceIt = source.iterator

    val (_, coresetTime) = timed {
      while (sourceIt.hasNext) {
        coreset.update(sourceIt.next())
      }
    }
    println(s"Coreset computed in $coresetTime nanoseconds")

    val points = coreset.points.toArray

    val (farthestSubset, farthestSubsetTime) = timed{
      FarthestPointHeuristic.run(points, source.k, source.distance)
    }
    println(s"Farthest heuristic computed in $farthestSubsetTime nanoseconds")

    val (matchingSubset, matchingSubsetTime) = timed{
      MatchingHeuristic.run(points, source.k, source.distance)
    }
    println(s"Matching heuristic computed in $matchingSubsetTime nanoseconds")

    val edgeDiversity = Diversity.edge(farthestSubset, source.distance)
    val cliqueDiversity = Diversity.clique(matchingSubset, source.distance)
    val treeDiversity = Diversity.tree(farthestSubset, source.distance)
    val starDiversity = Diversity.star(matchingSubset, source.distance)

    experiment.append("approximation",
      jMap(
        "certificate-edge"   -> source.edgeDiversity,
        "certificate-clique" -> source.cliqueDiversity,
        "certificate-tree"   -> source.treeDiversity,
        "certificate-star"   -> source.starDiversity,
        "computed-edge"      -> edgeDiversity,
        "computed-clique"    -> cliqueDiversity,
        "computed-tree"      -> treeDiversity,
        "computed-star"      -> starDiversity,
        "ratio-edge"         -> source.edgeDiversity.toDouble / edgeDiversity,
        "ratio-clique"       -> source.cliqueDiversity.toDouble / cliqueDiversity,
        "ratio-tree"         -> source.treeDiversity.toDouble / treeDiversity,
        "ratio-star"         -> source.starDiversity.toDouble / starDiversity
      ))

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
    val opts = new Conf(args)
    opts.verify()

    val sourcesList = opts.source().split(",")
    val dimList = opts.spaceDimension().split(",").map{_.toInt}
    val kList = opts.k().split(",").map{_.toInt}
    val numPointsList = opts.numPoints().split(",").map{_.toInt}
    val kernelSizeList = opts.kernelSize().split(",").map{_.toInt}

    val pl = new ProgressLogger("experiments")
    pl.displayFreeMemory = true
    pl.expectedUpdates =
      dimList.length*kList.length*numPointsList.length*kernelSizeList.length
    pl.start(s"Starting ${pl.expectedUpdates} experiments")
    for {
      sourceName   <- sourcesList
      dim      <- dimList
      k        <- kList
      n        <- numPointsList
      kernSize <- kernelSizeList
    } {
      try {
        val experiment = new Experiment()
          .tag("version", BuildInfo.version)
          .tag("git-revision", BuildInfo.gitRevision)
          .tag("git-revcount", BuildInfo.gitRevCount)
          .tag("source", sourceName)
          .tag("space-dimension", dim)
          .tag("k", k)
          .tag("num-points", n)
          .tag("kernel-size", kernSize)
          .tag("algorithm", "streaming")
        val source = PointSource(sourceName, dim, n, k, Distance.euclidean)
        run(source, kernSize, experiment)
        experiment.saveAsJsonFile()
        println(experiment.toSimpleString)
      } catch {
        case e: Exception =>
          println(s"Error: ${e.getMessage}")
          e.printStackTrace()
      }
      pl.update()
    }
    pl.stop("Done")
  }

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val source = opt[String](default = Some("random-gaussian-sphere"))

    lazy val spaceDimension = opt[String](default = Some("2"))

    lazy val k = opt[String](required = true)

    lazy val kernelSize = opt[String](required = true)

    lazy val numPoints = opt[String](required = true)

  }

}
