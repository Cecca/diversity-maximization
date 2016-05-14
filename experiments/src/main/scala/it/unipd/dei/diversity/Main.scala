package it.unipd.dei.diversity

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.diversity.ExperimentUtil._
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

    experiment.append("main",
      jMap(
        "throughput"         -> 1000.0 * cnt / time,
        "certificate-edge"   -> source.edgeDiversity,
        "certificate-clique" -> source.cliqueDiversity,
        "certificate-tree"   -> source.treeDiversity,
        "certificate-star"   -> source.starDiversity,
        "computed-edge"      -> Diversity.edge(points, k, distance),
        "computed-clique"    -> Diversity.clique(points, k, distance),
        "computed-tree"      -> Diversity.tree(points, k, distance),
        "computed-star"      -> Diversity.star(points, k, distance)
      ))
  }

  def main(args: Array[String]) {
    val opts = new Conf(args)
    opts.verify()

    val dim = opts.spaceDimension()
    val k = opts.k()
    val numPoints = opts.numPoints()
    val kernelSize = opts.kernelSize()

    val experiment = new Experiment()

    experiment
      .tag("version", BuildInfo.version)
      .tag("git-revision", BuildInfo.gitRevision)
      .tag("git-revcount", BuildInfo.gitRevCount)
      .tag("space-dimension", dim)
      .tag("k", k)
      .tag("num-points", numPoints)
      .tag("kernel-size", kernelSize)

    runRandomSpherePoints(dim, k, numPoints, kernelSize, experiment)

    experiment.saveAsJsonFile()
  }

}

class Conf(args: Array[String]) extends ScallopConf(args) {

  val spaceDimension = opt[Int](default = Some(2))

  val k = opt[Int](required = true)

  val kernelSize = opt[Int](required = true)

  val numPoints = opt[Int](required = true)

}