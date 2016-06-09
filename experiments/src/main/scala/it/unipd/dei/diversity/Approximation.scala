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
                              experiment: Experiment) = {

    println("Compute approximation for remote-edge")
    val (farthestSubsetCenters, _): (Option[IndexedSeq[T]], Long) =
      if (computeFarthest) {
        timed {
          val pts = if(coreset.kernel.length < k) {
            coreset.points
          } else {
            coreset.kernel
          }
          Some(FarthestPointHeuristic.run(pts, k, distance))
        }
      } else {
        (None, 0)
      }

    println("Compute approximation for remote-tree")
    val (farthestSubsetWDelegates, farthestSubsetTime): (Option[IndexedSeq[T]], Long) =
      if (computeFarthest) {
        timed {
          Some(FarthestPointHeuristic.run(coreset.points, k, distance))
        }
      } else {
        (None, 0)
      }

    println("Compute approximation for remote-clique and remote-star")
    val (matchingSubset, matchingSubsetTime): (Option[IndexedSeq[T]], Long) =
      if (computeMatching) {
        timed {
          Some(MatchingHeuristic.run(coreset.points, k, distance))
        }
      } else {
        (None, 0)
      }

    approxTable(
      farthestSubsetCenters, farthestSubsetWDelegates, matchingSubset, distance)
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

  def approxTable[T:ClassTag](farthestSubsetCenters: Option[IndexedSeq[T]],
                              farthestSubsetWDelegates: Option[IndexedSeq[T]],
                              matchingSubset: Option[IndexedSeq[T]],
                              distance: (T, T) => Double) = {

    val columns = mutable.ArrayBuffer[(String, Any)]()

    farthestSubsetCenters.foreach { fs =>
      val edgeDiversity = Diversity.edge(fs, distance)
      columns.append(
        "computed-edge" -> edgeDiversity
      )
    }

    farthestSubsetWDelegates.foreach { fs =>
      val treeDiversity = Diversity.tree(fs, distance)
      columns.append(
        "computed-tree" -> treeDiversity
      )
    }

    matchingSubset.foreach { ms =>
      val cliqueDiversity = Diversity.clique(ms, distance)
      val starDiversity   = Diversity.star(ms, distance)
      columns.append(
        "computed-clique" -> cliqueDiversity,
        "computed-star"   -> starDiversity
      )
    }

    if (columns.nonEmpty) {
      Some(jMap(columns: _*))
    } else {
      None
    }
  }


}
