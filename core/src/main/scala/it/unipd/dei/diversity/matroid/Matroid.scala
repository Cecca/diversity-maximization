package it.unipd.dei.diversity.matroid

import it.unipd.dei.diversity.IndexedSubset

import scala.collection.mutable
import scala.reflect.ClassTag

trait Matroid[T] {

  def isIndependent(elements: Seq[T]): Boolean

  def isIndependent(elements: IndexedSubset[T]): Boolean = {
    println("WARNING: Using unoptimized implementation of isIndependent")
    isIndependent(elements.toSet.toSeq)
  }

  def coreSetPoints(elements: Seq[T], k: Int): Seq[T]

  def independentSetOfSize(elements: Seq[T], k: Int): Seq[T] = {
    // FIXME optimize
    var is = Vector[T]()
    val elemIterator = elements.iterator
    while(is.size < k && elemIterator.hasNext) {
      val e = elemIterator.next()
      if (isIndependent(is :+ e)) {
        is = is :+ e
      }
    }
    is
  }

  def independentSetOfSize(elements: IndexedSeq[T], k: Int): IndexedSubset[T] = {
    val is = IndexedSubset(elements)
    var i = 0
    while (i < elements.size && is.size < k) {
      is.add(i)
      if(!isIndependent(is)) {
        is.remove(i)
      }
      i += 1
    }
    is
  }


}

/**
  * Encodes a partition matroid with a mapping between
  * categories and number of elements allowed for each category.
  * If a category is not in the mapping, then its allowed count is 0.
  */
class PartitionMatroid[T](val categories: Map[Int, Int],
                          val getCategory: T => Int) extends Matroid[T] {

  override def isIndependent(elements: Seq[T]): Boolean = {
    val counts = mutable.Map[Int, Int]()
    for (e <- elements) {
      val c = getCategory(e)
      counts(c) += 1
    }
    for ((cat, cnt) <- counts) {
      if (cnt > categories.getOrElse(cat, 0)) {
        return false
      }
    }
    true
  }

  override def coreSetPoints(elements: Seq[T], k: Int): Seq[T] = ???

}

class TransversalMatroid2[T:ClassTag, S](val sets: Array[S],
                                         val getSets: T => Seq[S]) extends Matroid[T] {

  override def isIndependent(elements: Seq[T]): Boolean = {
    if (elements.length > sets.length) {
      return false
    }
    // Return true only if all the elements in the given set are matched to some set
    maximumMatching(elements.toVector)._1 == elements.length
  }

  override def coreSetPoints(elements: Seq[T], k: Int): Seq[T] = {
    val elementsArr = elements.toArray // FIXME: Get rid of this
    // First, get an independent set of size k
    val is = independentSetOfSize(elementsArr, k)
    val numAdditionalPoints = is.size
    // Then, compute the set of delegates. For each set represented by
    // elements, we add at most numAdditionalPoints to the output.
    val output = IndexedSubset(elementsArr)
    val matchedSets = is.toSet.flatMap(x => getSets(x))
    val matchedSetsCounts = mutable.HashMap[S, Int]()
    for (eIdx <- is.supersetIndices; set <- getSets(elements(eIdx))) {
      if (matchedSets.contains(set) && matchedSetsCounts.getOrElse(set, 0) < numAdditionalPoints) {
        output.add(eIdx)
        matchedSetsCounts.update(set, matchedSetsCounts.getOrElse(set, 0) + 1)
      }
    }
    output.toVector
  }


  def maximumMatching(elements: IndexedSeq[T]): (Int, Iterator[(T, S)]) = {
    val visitedSets = IndexedSubset(sets) // The sets visited in a given iteration
    val representatives = Array.fill[Int](sets.length)(-1) // An array saying to which element is matched each set. -1 means no element matched

    var matchingSize = 0
    for(eIdx <- elements.indices) {
      visitedSets.clear()
      if (findMatchingFor(elements, eIdx, representatives, visitedSets)) {
        matchingSize += 1
      }
    }
    val matchingIterator = representatives.view
      .zipWithIndex
      .iterator
      .filter {case (eIdx, sIdx) => eIdx >= 0}
      .map {case (eIdx, sIdx) => (elements(eIdx), sets(sIdx))}

    (matchingSize, matchingIterator)
  }

  def findMatchingFor(elements: IndexedSeq[T],
                      eIdx: Int,
                      representatives: Array[Int],
                      visitedSets: IndexedSubset[S]): Boolean = {
    for (sIdx <- sets.indices) {
      // If there is an edge between the element and the current set,
      // and the current set is not visited
      if (getSets(elements(eIdx)).contains(sets(sIdx)) && !visitedSets.contains(sIdx)) {
        visitedSets.add(sIdx)
        // Try to make the element a representative of the set
        if (representatives(sIdx) < 0 || findMatchingFor(elements, representatives(sIdx), representatives, visitedSets)) {
          representatives(sIdx) = eIdx
          return true
        }
      }
    }
    false
  }

}

class TransversalMatroid[T](val sets: Array[Int],
                            val getSets: T => Seq[Int]) extends Matroid[T] {

  require(
    sets.zip(sets.tail).map({ case (x, y) => x <= y }).reduce(_ && _),
    s"The sets array should be sorted ${sets.mkString("[", ", ", "]")}")

  override def isIndependent(elements: Seq[T]): Boolean = {
    if (elements.length > sets.length) {
      return false
    }
    val matched = Array.ofDim[Boolean](sets.length)

    val elementsSets = elements.map(getSets(_).toArray).toArray
    // Return true only if all the elements in the given set are matched to some set
    TransversalMatroid.bipartiteMatchingSize(sets, elementsSets) == elements.length
  }

  override def coreSetPoints(elements: Seq[T], k: Int): Seq[T] = {
    // First, get an independent set of size k
    val is = independentSetOfSize(elements, k)
    val numAdditionalPoints = is.size
    // Then, compute the set of delegates. For each set represented by
    // elements, we add at most numAdditionalPoints to the output.
    val output = mutable.Set[T](is :_*)
    val matchedSets = Set[Int](is.flatMap(x => getSets(x)) :_*)
    val matchedSetsCounts = mutable.HashMap[Int, Int]()
    for (elem <- is; set <- getSets(elem)) {
      if (matchedSets.contains(set) && matchedSetsCounts.getOrElse(set, 0) < numAdditionalPoints) {
        output.add(elem)
        matchedSetsCounts.update(set, matchedSetsCounts.getOrElse(set, 0) + 1)
      }
    }
    output.toSeq
  }

}

object TransversalMatroid {

  @inline def indexOf(sets: Array[Int], s: Int): Int = java.util.Arrays.binarySearch(sets, s)

  def hasAugmentingPath(matched: Array[Int],
                        sets: Array[Int],
                        setsIncidentToElement: Array[Array[Int]],
                        elementIdx: Int,
                        seen: Array[Boolean]): Boolean = {
    // Try all the jobs adjacent to the given element
    for (s <- setsIncidentToElement(elementIdx)) {
      val sIdx = indexOf(sets, s)
      if (!seen(sIdx)) {
        seen(sIdx) = true

        if (matched(sIdx) < 0 || hasAugmentingPath(matched, sets, setsIncidentToElement, matched(sIdx), seen)) {
          matched(sIdx) = elementIdx
          return true
        }
      }
    }
    false
  }

  def bipartiteMatchingSize(sets: Array[Int], elements: Array[Array[Int]]): Int = {
    val matched = Array.fill[Int](sets.length)(-1)
    val seen = Array.fill[Boolean](sets.length)(false)
    var matchedCnt = 0
    for (eIdx <- elements.indices) {
      for (i <- seen.indices) {
        seen(i) = false
      }
      if (hasAugmentingPath(matched, sets, elements, eIdx, seen)) {
        matchedCnt += 1
      }
    }

    matchedCnt
  }

}
