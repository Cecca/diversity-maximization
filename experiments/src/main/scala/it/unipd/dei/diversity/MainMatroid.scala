package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import it.unipd.dei.diversity.ExperimentUtil.{jMap, timed}
import it.unipd.dei.diversity.matroid._
import it.unipd.dei.experiment.Experiment
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

import scala.collection.mutable
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
  
  def main(args: Array[String]) {
    val opts = new Opts(args)
    opts.verify()

    require(opts.categories.isDefined ^ opts.genres.isDefined ^ opts.topics.isDefined ^ opts.uniform.isDefined,
      "exactly one between categories, genres, cardinalit, and topics can be defined")

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

    val setup =
      if (opts.categories.isDefined) {
        new WikipediaExperiment(spark, opts.input(), opts.categories.get)
      } else if (opts.genres.isDefined) {
        new SongExperiment(spark, opts.input(), opts.genres())
      } else if (opts.topics.isDefined) {
        new WikipediaLDAExperiment(spark, opts.input())
      } else if (opts.uniform.isDefined) {
        opts.uniform() match {
          case "glove" => new GloVeExperiment(spark, opts.input(), opts.k())
        }
      } else {
        throw new IllegalArgumentException("Must provide at least one between categories, genres, or topics")
      }

    run(opts, setup, experiment)

    val counters = PerformanceMetrics.registry.getCounters.entrySet().iterator()
    while(counters.hasNext) {
      val c = counters.next()
      experiment.append("counters",
        jMap("name" -> c.getKey, "count" -> c.getValue.getCount))
    }

    println(experiment.toSimpleString)
    experiment.saveAsJsonFile(true)

  }

  private def run[T:ClassTag](opts: Opts, setup: ExperimentalSetup[T], experiment: Experiment): Any = {
    opts.algorithm() match {
      case "random" =>
        val dataset = setup.loadDataset().rdd.cache()
        // Force loading of dataset, so not to measure the loading time too.
        dataset.count()
        val k = opts.k()
        val ((solution, numberOfSamples), time) = timed {
          println("Taking first sample")
          var sample = dataset.takeSample(withReplacement = false, k)
          var numSamples = 1
          while (!setup.matroid.isIndependent(sample)) {
            println(s"Taking sample $numSamples")
            sample = dataset.takeSample(withReplacement = false, k)
            numSamples += 1
          }
          (sample, numSamples)
        }
        require(solution.size == opts.k(), "Solution of wrong size")

        experiment.append("result",
          jMap(
            "diversity" -> Diversity.clique(solution, setup.distance),
            "number-of-samples" -> numberOfSamples,
            "total-time" -> ExperimentUtil.convertDuration(time, TimeUnit.MILLISECONDS)))

        for (wp <- solution) {
          experiment.append("solution",
            jMap(setup.pointToMap(wp).toSeq: _*))
        }


      case "local-search" =>
        experiment.tag("gamma", opts.gamma())

        val localDataset: Array[T] = setup.loadLocally()
        val (solution, t) = timed {
          LocalSearch.remoteClique[T](
            localDataset, opts.k(), opts.gamma(), setup.matroid, setup.distance)
        }
        require(solution.size == opts.k(), "Solution of wrong size")

        experiment.append("times",
          jMap(
            "component" -> "local-search",
            "time"      -> ExperimentUtil.convertDuration(t, TimeUnit.MILLISECONDS)))
        experiment.append("result",
          jMap(
            "diversity" -> Diversity.clique(solution, setup.distance),
            "total-time" -> ExperimentUtil.convertDuration(t, TimeUnit.MILLISECONDS)))

        for (wp <- solution) {
          experiment.append("solution",
            jMap(setup.pointToMap(wp).toSeq: _*))
        }

      case "streaming" =>
        require(opts.tau.isDefined)
        val tau = opts.tau()
        val parallelism = opts.parallelism.get.getOrElse(spark.sparkContext.defaultParallelism)
        experiment.tag("tau", tau)
        var coresetSize: Option[Long] = None
        val datasetIter = setup.loadDataset().toLocalIterator()
        val dataset: mutable.ArrayBuffer[T] = mutable.ArrayBuffer()
        while (datasetIter.hasNext()) {
          dataset.append(datasetIter.next())
        }
        val streamingCoreset = Algorithm.streaming(
          dataset.iterator, opts.k(), tau, setup.matroid, setup.distance, experiment)
        val sizes = streamingCoreset.delegateSizes
        val originalCoresetSize: Int = sizes.sum
        println(s"Delegate set sizes:\n${sizes.mkString("\n")}")
        for ((size, i) <- sizes.zipWithIndex) {
          experiment.append("delegates",
            jMap(
              "componentId" -> i,
              "size" -> size
            )    
          )
        }

        val coreset =
          if (opts.sparsify()) {
            val (_c, _t) = timed {
              MapReduceCoreset.run(
                streamingCoreset.points.toArray, 
                tau, opts.k(), setup.matroid, setup.distance)
            }
            experiment.append("times",
              jMap(
                "component" -> "sparsification",
                "time"      -> ExperimentUtil.convertDuration(_t, TimeUnit.MILLISECONDS)))
            println(s"Sparsified coreset has ${_c.length} points")
            _c
          } else {
            streamingCoreset
          }

        val (solution, lsTime) = timed {
          LocalSearch.remoteClique[T](
            coreset.points,
            opts.k(), opts.gamma(), setup.matroid, setup.distance)
        }
        require(solution.size == opts.k(), "Solution of wrong size")
        require(setup.matroid.isIndependent(solution), "The solution is not an independent set!")
        experiment.append("times",
          jMap(
            "component" -> "local-search",
            "time"      -> ExperimentUtil.convertDuration(lsTime, TimeUnit.MILLISECONDS)))
        experiment.append("result",
          jMap(
            "coreset-size" -> originalCoresetSize,
            "diversity" -> Diversity.clique(solution, setup.distance)))

        for (wp <- solution) {
          experiment.append("solution",
            jMap(setup.pointToMap(wp).toSeq: _*))
        }

      case "mapreduce" =>
        require(opts.tau.isDefined)
        val tau = opts.tau()
        val tauParallel = opts.tauP.get.getOrElse(tau)
        val parallelism = opts.parallelism.get.getOrElse(spark.sparkContext.defaultParallelism)
        experiment.tag("tau", tau)
        experiment.tag("tau-parallel", tauParallel)
        experiment.tag("num-partitions", parallelism)
        experiment.tag("sparsify", opts.sparsify.isDefined)
        var coresetSize: Option[Long] = None
        val dataset = setup.loadDataset().rdd.repartition(parallelism).glom()
        val mrCoreset = Algorithm.mapReduce(
          dataset, tauParallel, opts.k(), setup.matroid, setup.distance, experiment)
        println(s"Computed coreset with ${mrCoreset.length} points and radius ${mrCoreset.radius}")
        val coreset =
          if (opts.sparsify()) {
            val (_c, _t) = timed {
              MapReduceCoreset.run(mrCoreset.points.toArray, tau, opts.k(), setup.matroid, setup.distance)
            }
            experiment.append("times",
              jMap(
                "component" -> "sparsification",
                "time"      -> ExperimentUtil.convertDuration(_t, TimeUnit.MILLISECONDS)))
            println(s"Sparsified coreset has ${_c.length} points")
            _c
          } else {
            mrCoreset
          }
        val (solution, lsTime) = timed {
          LocalSearch.remoteClique[T](
            coreset.points, opts.k(), 0.0, setup.matroid, setup.distance)
        }
        require(solution.size == opts.k(), "Solution of wrong size")
        require(setup.matroid.isIndependent(solution), "The solution is not an independent set!")
        experiment.append("times",
          jMap(
            "component" -> "local-search",
            "time"      -> ExperimentUtil.convertDuration(lsTime, TimeUnit.MILLISECONDS)))
        experiment.append("result",
          jMap(
            "diversity" -> Diversity.clique(solution, setup.distance),
            "large-coreset-size" -> mrCoreset.length,
            "small-coreset-size" -> coreset.length))

        for (wp <- solution) {
          experiment.append("solution",
            jMap(setup.pointToMap(wp).toSeq: _*))
        }



      case "sequential-coreset" =>
        val gamma = opts.gamma.get.getOrElse(0.0)
        experiment.tag("k'", opts.tau())
        experiment.tag("ls-subroutine-gamma", gamma)
        var coresetSize: Option[Long] = None
        val localDataset: Array[T] = setup.loadLocally()
        val ((solution, coresetTime, localSearchTime), totalTime) =
          timed {
            val (coreset, _coresetTime) = timed  {
              MapReduceCoreset.run(
                localDataset, opts.tau(), opts.k(), setup.matroid, setup.distance)
            }
            coresetSize = Some(coreset.length)
            println(s"Built coreset with ${coreset.length} over ${localDataset.length} points")
            val (sol, _lsTime) = timed {
              LocalSearch.remoteClique[T](
                coreset.points, opts.k(), gamma, setup.matroid, setup.distance)
            }
            (sol, _coresetTime, _lsTime)
          }

        require(solution.size == opts.k(), "Solution of wrong size")
        experiment.append("times",
          jMap(
            "component" -> "local-search",
            "time"      -> ExperimentUtil.convertDuration(localSearchTime, TimeUnit.MILLISECONDS)))
        experiment.append("times",
          jMap(
            "component" -> "coreset",
            "time"      -> ExperimentUtil.convertDuration(coresetTime, TimeUnit.MILLISECONDS)))
        experiment.append("result",
          jMap(
            "diversity" -> Diversity.clique(solution, setup.distance),
            "coreset-size" -> coresetSize.get,
            "total-time" -> ExperimentUtil.convertDuration(totalTime, TimeUnit.MILLISECONDS),
            "coreset-time" -> ExperimentUtil.convertDuration(coresetTime, TimeUnit.MILLISECONDS),
            "local-search-time" -> ExperimentUtil.convertDuration(localSearchTime, TimeUnit.MILLISECONDS)))

        for (wp <- solution) {
          experiment.append("solution",
            jMap(setup.pointToMap(wp).toSeq: _*))
        }

      case "clustering-radius" =>
        require(opts.epsilon.isDefined)
        experiment.tag("epsilon", opts.epsilon())
        val localDataset: Array[T] = setup.loadLocally()
        val coreset = withRadiusExp[T](
          localDataset, opts.epsilon(), Random.nextInt(localDataset.length), setup.distance, experiment)


    }
  }

  class Opts(args: Array[String]) extends ScallopConf(args) {

    lazy val algorithm = opt[String](default = Some("local-search"))

    lazy val k = opt[Int](name="target", short='k', required = true)

    lazy val gamma = opt[Double](default = Some(0.0))

    lazy val tau = opt[Int]()

    lazy val tauP = opt[Int]()

    lazy val parallelism = opt[Int]()

    lazy val epsilon = opt[Double]()

    lazy val sparsify = toggle(
      default=Some(true),
      descrYes = "whether to sparsify the coreset resulting from the MapReduce algorithm")

    lazy val input = opt[String](required = true)

    lazy val categories = opt[String](required = false, argName = "FILE")

    lazy val genres = opt[String](required = false, argName = "FILE")

    lazy val topics = toggle()

    lazy val uniform = opt[String](argName = "DATA TYPE", validate = Set("glove").contains,
                                       descr = "Use a cardinality matroid, and specify the data type")

    lazy val diameter = opt[Double](required = false, argName = "DELTA")

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
