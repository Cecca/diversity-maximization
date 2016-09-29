// diversity-maximization: Diversity maximization in Streaming and MapReduce
// Copyright (C) 2016  Matteo Ceccarello <ceccarel@dei.unipd.it>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package it.unipd.dei.diversity

import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.experiment.Experiment

import scala.collection.mutable
import scala.reflect.ClassTag

object Approximation {

  def approximate[T:ClassTag](coreset: Coreset[T],
                              k: Int,
                              kernelSize: Int,
                              distance: (T, T) => Double,
                              computeFarthest: Boolean,
                              computeMatching: Boolean,
                              runNumber: Int,
                              experiment: Experiment): Unit =
    approximate(coreset, k, kernelSize, distance, computeFarthest, computeMatching, runNumber, None, experiment)

  def approximate[T:ClassTag](coreset: Coreset[T],
                              k: Int,
                              kernelSize: Int,
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
          val kern = coreset.kernel
          val pts = if(kern.length < kernelSize) {
            kern ++ coreset.delegates.take(kernelSize - kern.length)
          } else {
            kern
          }
          require(k <= pts.size && pts.size <= kernelSize,
            s"Should not compute remote-edge on ${pts.size} points, when $k <= x <= $kernelSize are required")
          println(s"Compute approximation for remote-edge (${pts.length} points)")
          val (bestApprox, set) = (0 until math.min(runNumber, pts.length)).par.map { i =>
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
        val (bestApprox, set) = (0 until math.min(runNumber, points.length)).par.map { i =>
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
        edgeSolution match {
          case None => Some((Diversity.clique(sub, distance), Diversity.star(sub, distance), sub))
          case Some((_, gmmSet)) =>
            val divCliqueGmm = Diversity.clique(gmmSet.toVector, distance)
            val divStarGmm = Diversity.star(gmmSet.toVector, distance)

            val divCliqueMatching = Diversity.clique(sub, distance)
            val divStarMatching = Diversity.star(sub, distance)
            if (divCliqueMatching < divCliqueGmm) {
              println("The GMM heuristic found a better solution for remote-clique, using it.")
              Some((divCliqueGmm, divStarGmm, gmmSet))
            } else {
              Some((divCliqueMatching, divStarMatching, sub))
            }
        }
      }
    } else if (computeFarthest) {
      println("Using GMM solution for remote-clique and remote-star")
      val edgeSet = edgeSolution.get._2.toVector
      val treeSet = treeSolution.get._2.toVector
      val edgeSetDiv = Diversity.clique(edgeSet, distance)
      val treeSetDiv = Diversity.clique(treeSet, distance)
      if (edgeSetDiv >= treeSetDiv) {
        (Some((edgeSetDiv, Diversity.star(edgeSet, distance), edgeSet)), 0l)
      } else {
        (Some((treeSetDiv, Diversity.star(treeSet, distance), treeSet)), 0l)
      }
    } else {
      (None, 0)
    }

    approxTable(edgeSolution, treeSolution, matchingSolution)
      .foreach { row =>
        experiment.append("approximation", row)
      }

    solutionTable(pointToRow, edgeSolution, treeSolution, matchingSolution, experiment)

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
                              matchingSolution: Option[(Double, Double, Seq[T])]) = {

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

  def solutionTable[T:ClassTag](pointToRow: Option[T => Map[String, Any]],
                                edgeSolution: Option[(Double, Seq[T])],
                                treeSolution: Option[(Double, Seq[T])],
                                matchingSolution: Option[(Double, Double, Seq[T])],
                                experiment: Experiment) = pointToRow match {
    case None => // Do nothing!
    case Some(func) =>
      edgeSolution.foreach { case (_, solution) =>
        solution.map(func).foreach { row =>
          experiment.append("solution-remote-edge", jMap(row.toSeq :_*))
        }
      }
      treeSolution.foreach { case (_, solution) =>
        solution.map(func).foreach { row =>
          experiment.append("solution-remote-tree", jMap(row.toSeq :_*))
        }
      }
      matchingSolution.foreach { case (_, _, solution) =>
        solution.map(func).foreach { row =>
          experiment.append("solution-remote-clique", jMap(row.toSeq :_*))
          experiment.append("solution-remote-star", jMap(row.toSeq :_*))
        }
      }
  }

}
