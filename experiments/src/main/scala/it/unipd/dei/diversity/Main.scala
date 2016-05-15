package it.unipd.dei.diversity

import it.unimi.dsi.logging.ProgressLogger
import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.experiment.Experiment
import org.rogach.scallop.ScallopConf

object Main {

  def runRandomSpherePoints(dim: Int,
                            k: Int,
                            numPoints: Int,
                            kernelSize: Int,
                            experiment: Experiment) = {
    val distance: (Point, Point) => Double = Distance.euclidean
    val source = new RandomSpherePointSource(dim, numPoints, k, distance)
    val coreset = new StreamingCoreset(kernelSize, k, distance)

    var cnt = 0
    val start = System.currentTimeMillis()

    while(source.hasNext) {
      coreset.update(source.next())
      cnt += 1
    }

    val end = System.currentTimeMillis()
    val time = end - start

    val points = coreset.points.toArray

//    println("Apply greedy algorithms on the coreset")
    val farthestSubset = FarthestPointHeuristic.run(points, k, distance)
    val matchingSubset = MatchingHeuristic.run(points, k, distance)

    experiment.append("main",
      jMap(
        "throughput"         -> 1000.0 * cnt / time,
        "certificate-edge"   -> source.edgeDiversity,
        "certificate-clique" -> source.cliqueDiversity,
        "certificate-tree"   -> source.treeDiversity,
        "certificate-star"   -> source.starDiversity,
        "computed-edge"      -> Diversity.edge(farthestSubset, distance),
        "computed-clique"    -> Diversity.clique(matchingSubset, distance),
        "computed-tree"      -> Diversity.tree(farthestSubset, distance),
        "computed-star"      -> Diversity.star(matchingSubset, distance)
      ))
  }

  def main(args: Array[String]) {
    val opts = new Conf(args)
    opts.verify()

    val dimList = opts.spaceDimension().split(",").map{_.toInt}
    val kList = opts.k().split(",").map{_.toInt}
    val numPointsList = opts.numPoints().split(",").map{_.toInt}
    val kernelSizeList = opts.kernelSize().split(",").map{_.toInt}

    val pl = new ProgressLogger("experiments")
    pl.expectedUpdates =
      dimList.length*kList.length*numPointsList.length*kernelSizeList.length
    pl.start(s"Starting ${pl.expectedUpdates} experiments")
    for {
      dim      <- dimList
      k        <- kList
      n        <- numPointsList
      kernSize <- kernelSizeList
    } {
      try {
//        println(s"Stream of $n points: looking for $k most diverse (kernel size $kernSize)")
        val experiment = new Experiment()
          .tag("version", BuildInfo.version)
          .tag("git-revision", BuildInfo.gitRevision)
          .tag("git-revcount", BuildInfo.gitRevCount)
          .tag("space-dimension", dim)
          .tag("k", k)
          .tag("num-points", n)
          .tag("kernel-size", kernSize)
        runRandomSpherePoints(dim, k, n, kernSize, experiment)
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

}

class Conf(args: Array[String]) extends ScallopConf(args) {

  lazy val spaceDimension = opt[String](default = Some("2"))

  lazy val k = opt[String](required = true)

  lazy val kernelSize = opt[String](required = true)

  lazy val numPoints = opt[String](required = true)

}