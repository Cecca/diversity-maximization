package it.unipd.dei.diversity

import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.experiment.Experiment

import scala.collection.mutable
import scala.reflect.ClassTag

object Approximation {

  def approximate[T:ClassTag](coreset: Coreset[T],
                              k: Int,
                              distance: (T, T) => Double,
                              computeFarthest: Boolean,
                              computeMatching: Boolean,
                              runNumber: Int,
                              experiment: Experiment): Unit =
    approximate(coreset, k, distance, computeFarthest, computeMatching, runNumber, None, experiment)

  def approximate[T:ClassTag](coreset: Coreset[T],
                              k: Int,
                              distance: (T, T) => Double,
                              computeFarthest: Boolean,
                              computeMatching: Boolean,
                              runNumber: Int,
                              pointToRow: Option[T => Map[String, Any]],
                              experiment: Experiment): Unit = {
    require(runNumber > 0)

    val (edgeSolution, _): (Option[(Double, Seq[T])], Long) =
      if (computeFarthest) {
        timed {
          val pts = if(coreset.kernel.length < k) {
            coreset.points
          } else {
            coreset.kernel
          }
          println(s"Compute approximation for remote-edge (${pts.length} points)")
          val (bestApprox, set) = (0 until math.min(runNumber, pts.length)).map { i =>
            print("|")
            val sub = FarthestPointHeuristic.run(pts, k, i, distance)
            (Diversity.edge(sub, distance), sub)
          }.maxBy(_._1)
          println()
          Some((bestApprox, set))
        }
      } else {
        (None, 0)
      }

    val (treeSolution, farthestSubsetTime): (Option[(Double, Seq[T])], Long) =
    if (computeFarthest) {
      val points = coreset.points
      println(s"Compute approximation for remote-tree (${points.length} points)")
      timed {
        val (bestApprox, set) = (0 until math.min(runNumber, points.length)).map { i =>
          print("|")
          val sub = FarthestPointHeuristic.run(points, k, i, distance)
          (Diversity.tree(sub, distance), sub)
        }.maxBy(_._1)
        println()
        Some((bestApprox, set))
      }
    } else {
      (None, 0)
    }

    val (matchingSolution, matchingSubsetTime): (Option[(Double, Double, Seq[T])], Long) =
    if (computeMatching) {
      val points = coreset.points
      println(s"Compute approximation for remote-clique and remote-star (${points.length} points)")
      timed {
        val sub = MatchingHeuristic.run(points, k, distance)
        Some((Diversity.clique(sub, distance), Diversity.star(sub, distance), sub))
      }
    } else {
      (None, 0)
    }

    approxTable(edgeSolution, treeSolution, matchingSolution)
      .foreach { row =>
        experiment.append("approximation", row)
      }

    experiment.append("times",
      jMap(
        "component" -> "farthest",
        "time" -> convertDuration(farthestSubsetTime, reportTimeUnit)
      ))
    experiment.append("times",
      jMap(
        "component" -> "matching",
        "time" -> convertDuration(matchingSubsetTime, reportTimeUnit)
    ))

  }

  def approxTable[T:ClassTag](edgeSolution: Option[(Double, Seq[T])],
                              treeSolution: Option[(Double, Seq[T])],
                              matchingSolution: Option[(Double,Double, Seq[T])]) = {

    val columns = mutable.ArrayBuffer[(String, Any)]()

    edgeSolution.foreach { case (div, _) =>
      columns.append(
        "computed-edge" -> div
      )
    }

    treeSolution.foreach { case (div, _) =>
      columns.append(
        "computed-tree" -> div
      )
    }

    matchingSolution.foreach { case (cliqueDiv, starDiv, _) =>
      columns.append(
        "computed-clique" -> cliqueDiv,
        "computed-star" -> starDiv
      )
    }

    if (columns.nonEmpty) {
      Some(jMap(columns: _*))
    } else {
      None
    }
  }

}
