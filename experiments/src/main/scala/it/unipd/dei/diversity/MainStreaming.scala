package it.unipd.dei.diversity

import it.unimi.dsi.logging.ProgressLogger
import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.diversity.source.PointSource
import it.unipd.dei.experiment.Experiment
import org.rogach.scallop.ScallopConf

object MainStreaming {

  def run(source: PointSource, kernelSize: Int, experiment: Experiment) = {
    val coreset = new StreamingCoreset(kernelSize, source.k, source.distance)
    val sourceIt = source.iterator

    var cnt = 0
    var start = System.currentTimeMillis()
    while(sourceIt.hasNext) {
      coreset.update(sourceIt.next())
      cnt += 1
    }
    var end = System.currentTimeMillis()
    val throughput = 1000.0 * cnt / (end - start)
    println(s"Coreset computed in ${end - start} milliseconds ($throughput points/sec)")

    val points = coreset.points.toArray

    start = System.currentTimeMillis()
    val farthestSubset = FarthestPointHeuristic.run(points, source.k, source.distance)
    end = System.currentTimeMillis()
    println(s"Farthest point heuristic computed in ${end - start} milliseconds")

    start = System.currentTimeMillis()
    val matchingSubset = MatchingHeuristic.run(points, source.k, source.distance)
    end = System.currentTimeMillis()
    println(s"Matching heuristic computed in ${end - start} milliseconds")

    experiment.append("main",
      jMap(
        "throughput"         -> throughput,
        "certificate-edge"   -> source.edgeDiversity,
        "certificate-clique" -> source.cliqueDiversity,
        "certificate-tree"   -> source.treeDiversity,
        "certificate-star"   -> source.starDiversity,
        "computed-edge"      -> Diversity.edge(farthestSubset, source.distance),
        "computed-clique"    -> Diversity.clique(matchingSubset, source.distance),
        "computed-tree"      -> Diversity.tree(farthestSubset, source.distance),
        "computed-star"      -> Diversity.star(matchingSubset, source.distance)
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
