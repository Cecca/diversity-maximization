package it.unipd.dei.diversity

import it.unipd.dei.diversity.source.PointSource
import it.unipd.dei.experiment.Experiment
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import ExperimentUtil._

object MainSpark {

  /**
    * Merge two sorted arrays
    */
  def merge(a: Array[Point], b: Array[Point]): Array[Point] = {
    var i = 0
    var aIdx = 0
    var bIdx = 0

    val result: Array[Point] = Array.ofDim(a.length+b.length)

    // merge the first elements
    while(aIdx < a.length && bIdx < b.length) {
      if (a(aIdx) == b(bIdx)) {
        result(i) = a(aIdx)
        aIdx += 1
        bIdx += 1
      } else if(a(aIdx).compareTo(b(bIdx)) < 0) {
        result(i) = a(aIdx)
        aIdx += 1
      } else {
        result(i) = b(bIdx)
        bIdx += 1
      }
      i += 1
    }

    // merge the remaining
    while(aIdx < a.length) {
      result(i) = a(aIdx)
      aIdx += 1
      i += 1
    }
    while(bIdx < b.length) {
      result(i) = b(bIdx)
      bIdx += 1
      i += 1
    }

    result.take(i)
  }


  def run(sc: SparkContext,
          source: PointSource,
          kernelSize: Int,
          numDelegates: Int,
          distance: (Point, Point) => Double,
          experiment: Experiment) = {
    val input = sc.parallelize(source.toSeq)
    val points = input.mapPartitions { points =>
      val pointsArr: Array[Point] = points.toArray
      val coreset = MapReduceCoreset.run(pointsArr, kernelSize, numDelegates, distance)
      Iterator(coreset.sorted)
    }.reduce { (a, b) =>
      merge(a, b)
    }

    var start = System.currentTimeMillis()
    val farthestSubset = FarthestPointHeuristic.run(points, source.k, source.distance)
    var end = System.currentTimeMillis()
    println(s"Farthest point heuristic computed in ${end - start} milliseconds")

    start = System.currentTimeMillis()
    val matchingSubset = MatchingHeuristic.run(points, source.k, source.distance)
    end = System.currentTimeMillis()
    println(s"Matching heuristic computed in ${end - start} milliseconds")

    experiment.append("main",
      jMap(
//        "throughput"         -> throughput,
        "certificate-edge"   -> source.edgeDiversity,
        "certificate-clique" -> source.cliqueDiversity,
        "certificate-tree"   -> source.treeDiversity,
        "certificate-star"   -> source.starDiversity,
        "computed-edge"      -> Diversity.edge(farthestSubset, source.distance),
        "computed-clique"    -> Diversity.clique(matchingSubset, source.distance),
        "computed-tree"      -> Diversity.tree(farthestSubset, source.distance),
        "computed-star"      -> Diversity.star(matchingSubset, source.distance)
      ))
    println(experiment.toSimpleString)
  }

  def main(args: Array[String]) {
    val opts = new Conf(args)
    opts.verify()

    val sourcesList = opts.source().split(",")
    val dimList = opts.spaceDimension().split(",").map{_.toInt}
    val kList = opts.delegates().split(",").map{_.toInt}
    val numPointsList = opts.numPoints().split(",").map{_.toInt}
    val kernelSizeList = opts.kernelSize().split(",").map{_.toInt}

    val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("MapReduce coresets")
    val sc = new SparkContext(sparkConfig)
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
        val source = PointSource(sourceName, dim, n, k, Distance.euclidean)
        run(sc, source, kernSize, k, Distance.euclidean, experiment)
        experiment.saveAsJsonFile()
      } catch {
        case e: Exception =>
          println(s"Error: ${e.getMessage}")
          e.printStackTrace()
      }
    }

    sc.stop()
  }

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val source = opt[String](default = Some("random-gaussian-sphere"))

    lazy val spaceDimension = opt[String](default = Some("2"))

    lazy val delegates = opt[String](required = true)

    lazy val kernelSize = opt[String](required = true)

    lazy val numPoints = opt[String](required = true)

  }

}
