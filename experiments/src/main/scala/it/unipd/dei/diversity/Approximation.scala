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
                              experiment: Experiment) = {
    require(runNumber > 0)

    val (edgeDiv, _): (Option[Double], Long) =
      if (computeFarthest) {
        timed {
          val pts = if(coreset.kernel.length < k) {
            coreset.points
          } else {
            coreset.kernel
          }
          println(s"Compute approximation for remote-edge (${pts.length} points)")
          val bestApprox = (0 until math.min(runNumber, pts.length)).map { i =>
            print("|")
            val sub = FarthestPointHeuristic.run(pts, k, i, distance)
            Diversity.edge(sub, distance)
          }.max
          println()
          Some(bestApprox)
        }
      } else {
        (None, 0)
      }

    val (treeDiv, farthestSubsetTime): (Option[Double], Long) =
    if (computeFarthest) {
      val points = coreset.points
      println(s"Compute approximation for remote-tree (${points.length} points)")
      timed {
        val bestApprox = (0 until math.min(runNumber, points.length)).map { i =>
          print("|")
          val sub = FarthestPointHeuristic.run(points, k, i, distance)
          Diversity.tree(sub, distance)
        }.max
        println()
        Some(bestApprox)
      }
    } else {
      (None, 0)
    }

    val ((cliqueDiv, starDiv), matchingSubsetTime): ((Option[Double], Option[Double]), Long) =
    if (computeMatching) {
      val points = coreset.points
      println(s"Compute approximation for remote-clique and remote-star (${points.length} points)")
      timed {
        val sub = MatchingHeuristic.run(points, k, distance)
        (Some(Diversity.clique(sub, distance)), Some(Diversity.star(sub, distance)))
        }
      } else {
        ((None, None), 0)
      }

    approxTable(edgeDiv, treeDiv, cliqueDiv, starDiv)
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

  def approxTable[T:ClassTag](edgeDiv: Option[Double],
                              treeDiv: Option[Double],
                              cliqueDiv: Option[Double],
                              starDiv: Option[Double]) = {

    val columns = mutable.ArrayBuffer[(String, Any)]()

    edgeDiv.foreach { div =>
      columns.append(
        "computed-edge" -> div
      )
    }

    treeDiv.foreach { div =>
      columns.append(
        "computed-tree" -> div
      )
    }

    cliqueDiv.foreach { div =>
      columns.append(
        "computed-clique" -> div
      )
    }

    starDiv.foreach { div =>
      columns.append(
        "computed-star" -> div
      )
    }


    if (columns.nonEmpty) {
      Some(jMap(columns: _*))
    } else {
      None
    }
  }

}
