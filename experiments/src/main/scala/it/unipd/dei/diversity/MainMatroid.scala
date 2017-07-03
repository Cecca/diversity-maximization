package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Counter
import it.unipd.dei.diversity.ExperimentUtil.{jMap, timed}
import it.unipd.dei.diversity.matroid.{Matroid, TransversalMatroid}
import it.unipd.dei.diversity.wiki.WikiPage
import it.unipd.dei.experiment.Experiment
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import it.unipd.dei.diversity.performanceMetrics
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Random

object MainMatroid {

  // Set up Spark lazily, it will be initialized only if the algorithm needs it.
  lazy val spark = {
    val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("Matroid diversity")
    val _s = SparkSession.builder()
      .config(sparkConfig)
      .getOrCreate()
    _s.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    _s
  }

  private def farthestFrom[T:ClassTag](points: RDD[T],
                                       x: T,
                                       threshold: Double,
                                       matroid: Matroid[T],
                                       distance: (T, T) => Double): Double = {
    points.flatMap({y =>
      val d = distance(x, y)
      if (d >= threshold && matroid.isIndependent(Seq(x, y))) {
        Iterator(d)
      } else {
        Iterator.empty
      }
    }).max()
  }

  def diameterLowerBound[T:ClassTag](points: RDD[T],
                                     matroid: Matroid[T],
                                     distance: (T, T) => Double)(implicit ordering: Ordering[T]): Double = {
    val combinations = points
      .cartesian(points)
      .filter({case (x, y) => ordering.compare(x, y) < 0})
      .map({case k@(x, y) => (distance(x, y), k)})
    val (d, (x1, x2)) = combinations.max()(Ordering.by(_._1))
    if (matroid.isIndependent(Array(x1, x2))) {
      println("The farthest points form an independent set")
      return d
    }
    println("We have to seek another point for the diameter estimate")
    val threshold = d/2
    val dFromX1 = farthestFrom(points, x1, threshold, matroid, distance)
    val dFromX2 = farthestFrom(points, x2, threshold, matroid, distance)
    math.max(dFromX1, dFromX2)
  }

  def cliqueDiversity[T](subset: IndexedSubset[T],
                         distance: (T, T) => Double): Double = {
    val n = subset.superSet.length
    var currentDiversity: Double = 0
    var i = 0
    while (i<n) {
      if (subset.contains(i)) {
        var j = i + 1
        while (j < n) {
          if (subset.contains(j)) {
            currentDiversity += distance(subset.get(i).get, subset.get(j).get)
          }
          j += 1
        }
      }
      i += 1
    }
    currentDiversity
  }

  private def collectLocally(data: Dataset[WikiPage], numElements: Long): Array[WikiPage] = {
    val dataIt = data.toLocalIterator()
    val localDataset: Array[WikiPage] = Array.ofDim[WikiPage](numElements.toInt)
    var i = 0
    while (dataIt.hasNext) {
      localDataset(i) = dataIt.next()
      i += 1
    }
    println("Collected dataset locally")
    data.unpersist(blocking = true)
    localDataset
  }

  def main(args: Array[String]) {
    val opts = new Opts(args)
    opts.verify()

    val experiment = new Experiment()
      .tag("input", opts.input())
      .tag("k", opts.k())
      .tag("algorithm", opts.algorithm())
      .tag("version", BuildInfo.version)
      .tag("git-revision", BuildInfo.gitRevision)
      .tag("git-revcount", BuildInfo.gitRevCount)
      .tag("git-branch", BuildInfo.gitBranch)
    for ((k, v) <- SerializationUtils.metadata(opts.input())) {
      experiment.tag("input." + k, v)
    }

    val distance: (WikiPage, WikiPage) => Double = WikiPage.distanceArbitraryComponents

    import spark.implicits._
    val dataset = spark.read.parquet(opts.input()).as[WikiPage].cache()
    val categories =
      if (opts.categories.isDefined) {
        val _cats = Source.fromFile(opts.categories()).getLines().toArray
        experiment.tag("query-categories", _cats)
        _cats
      } else {
        val _cats = dataset.select("categories").as[Seq[String]].flatMap(identity).distinct().collect()
        experiment.tag("query-categories", "*all*")
        _cats
      }
    val matroid = new TransversalMatroid[WikiPage, String](categories, _.categories)

    val brCategories = spark.sparkContext.broadcast(categories.toSet)
    val filteredDataset = dataset.flatMap { wp =>
      val cs = brCategories.value
      val cats = wp.categories.filter(cs.contains)
      if (cats.nonEmpty) {
        Iterator( wp.copy(categories = cats) )
      } else {
        Iterator.empty
      }
    }.mapPartitions(Random.shuffle(_)).cache()
    val numElements = filteredDataset.count()
    println(s"The filtered dataset has $numElements elements")
    dataset.unpersist(blocking = true)
    experiment.tag("filtered dataset size", numElements)


    opts.algorithm() match {
      case "local-search" =>
        experiment.tag("gamma", opts.gamma())

        val localDataset: Array[WikiPage] = collectLocally(filteredDataset, numElements)
        val (solution, t) = timed {
          LocalSearch.remoteClique[WikiPage](
            localDataset, opts.k(), opts.gamma(), matroid, distance)
        }

        experiment.append("performance",
          jMap(
            "diversity" -> Diversity.clique(solution, distance),
            "time" -> ExperimentUtil.convertDuration(t, TimeUnit.MILLISECONDS)))

        for (wp <- solution) {
          experiment.append("solution",
            jMap(
              "title" -> wp.title,
              "categories" -> wp.categories))
        }

      case "sequential-coreset" =>
        var coresetSize: Option[Long] = None
        val (solution, time) =
          if (opts.kernelSize.isDefined) {
            experiment.tag("coreset-type", "with-cardinality")
            experiment.tag("k'", opts.kernelSize())
            val localDataset: Array[WikiPage] = collectLocally(filteredDataset, numElements)
            timed {
              val coreset = MapReduceCoreset.run(
                localDataset, opts.kernelSize(), opts.k(), matroid, distance)
              coresetSize = Some(coreset.length)
              LocalSearch.remoteClique[WikiPage](
                localDataset, opts.k(), 0.0, matroid, distance)
            }
          } else if(opts.epsilon.isDefined) {
            experiment.tag("coreset-type", "with-radius")
            experiment.tag("epsilon", opts.epsilon())
            val localDataset: Array[WikiPage] = collectLocally(filteredDataset, numElements)
            implicit val ord: Ordering[WikiPage] = Ordering.by(page => page.id)
            val delta = diameterLowerBound[WikiPage](filteredDataset.rdd, matroid, distance)
            val radius = (opts.epsilon() / 2) * (delta / (2*opts.k()))
            println(s"Building coreset with radius $radius (delta=$delta, epsilon=${opts.epsilon()}, k=${opts.k()})")
            PerformanceMetrics.reset()
            timed {
              val coreset = MapReduceCoreset.withRadius(
                localDataset, radius, opts.k(), matroid, distance)
              println(s"Built coreset with ${coreset.points.size} over ${localDataset.length} points")
              coresetSize = Some(coreset.length)
              LocalSearch.remoteClique[WikiPage](
                coreset.points, opts.k(), 0.0, matroid, distance)
            }
          } else {
            throw new IllegalArgumentException(
              "You have to specify either the kernel size or epsilon when running the sequential coreset")
          }

        experiment.append("performance",
          jMap(
            "diversity" -> Diversity.clique(solution, distance),
            "coreset-size" -> coresetSize.get,
            "time" -> ExperimentUtil.convertDuration(time, TimeUnit.MILLISECONDS)))

        for (wp <- solution) {
          experiment.append("solution",
            jMap(
              "title" -> wp.title,
              "categories" -> wp.categories))
        }

      case "clustering-radius" =>
        require(opts.epsilon.isDefined)
        experiment.tag("epsilon", opts.epsilon())
        val localDataset: Array[WikiPage] = collectLocally(filteredDataset, numElements)
        val coreset = withRadiusExp[WikiPage](
                localDataset, opts.epsilon(), Random.nextInt(localDataset.length), distance, experiment)


    }

    val counters = PerformanceMetrics.registry.getCounters.entrySet().iterator()
    while(counters.hasNext) {
      val c = counters.next()
      experiment.append("counters",
        jMap("name" -> c.getKey, "count" -> c.getValue.getCount))
    }

    println(experiment.toSimpleString)
    experiment.saveAsJsonFile(true)

  }

  class Opts(args: Array[String]) extends ScallopConf(args) {

    lazy val algorithm = opt[String](default = Some("local-search"))

    lazy val k = opt[Int](name="target", short='k', required = true)

    lazy val gamma = opt[Double](default = Some(0.0))

    lazy val kernelSize = opt[Int](short='s')

    lazy val epsilon = opt[Double]()

    // TODO Use this option
    lazy val approxRuns = opt[Int](default = Some(1))

    lazy val input = opt[String](required = true)

    lazy val categories = opt[String](required = false, argName = "FILE")

  }

  // Quick and dirty experiment to check how the radius decreases when doing a clustering
  def withRadiusExp[T: ClassTag](points: IndexedSeq[T],
                                 epsilon: Double,
                                 startIdx: Int,
                                 distance: (T, T) => Double,
                                 experiment: Experiment): IndexedSeq[T] = {
    val n = points.size
    val minDist = Array.fill(n)(Double.PositiveInfinity)
    val centers = IndexedSubset.apply(points)
    // Init the result with an arbitrary point
    centers.add(startIdx)
    var i = 0
    var radius: Double = 0d
    var nextCenter = 0
    while (i < n) {
      val d = distance(points(startIdx), points(i))
      minDist(i) = d
      if (d > radius) {
        radius = d
        nextCenter = i
      }
      i += 1
    }

    experiment.append("clustering-radius",
      jMap(
        "iteration" -> 0,
        "radius" -> radius))

    var iteration = 1
    while (radius > epsilon && centers.size != n) {
      val center = nextCenter
      centers.add(center)
      radius = 0.0
      i = 0
      // Re-compute the radius and find the farthest node
      while (i < n) {
        val d = distance(points(center), points(i))
        if (d < minDist(i)) {
          minDist(i) = d
        }
        if (minDist(i) > radius) {
          radius = minDist(i)
          nextCenter = i
        }
        i += 1
      }
      println(s"[$iteration] r=$radius")
      experiment.append("clustering-radius",
        jMap(
          "iteration" -> iteration,
          "radius" -> radius))
      iteration += 1
    }
    centers.toVector
  }


}
