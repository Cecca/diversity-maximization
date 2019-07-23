package it.unipd.dei.diversity.matroid

import java.util.NoSuchElementException

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import it.unipd.dei.diversity._

import scala.collection.mutable
import scala.reflect.ClassTag

trait Matroid[T] extends Serializable {

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

class CardinalityMatroid[T](val cardinality: Int) extends Matroid[T]{
  override def isIndependent(elements: Seq[T]): Boolean = elements.size <= cardinality

  override def isIndependent(elements: IndexedSubset[T]): Boolean = elements.size <= cardinality

  override def coreSetPoints(elements: Seq[T], k: Int): Seq[T] = independentSetOfSize(elements, k)
}

/**
  * Encodes a partition matroid with a mapping between
  * categories and number of elements allowed for each category.
  * If a category is not in the mapping, then its allowed count is 0.
  */
class PartitionMatroid[T](val categories: Map[String, Int],
                          val getCategory: T => String) extends Matroid[T] {

  override def isIndependent(elements: IndexedSubset[T]): Boolean = {
    PerformanceMetrics.matroidOracleCounterInc()
    val cs = new Object2IntOpenHashMap[String](categories.size)
    cs.defaultReturnValue(0)
    for (i <- elements.supersetIndices) {
      val e = elements.superSet(i)
      val c = getCategory(e)
      cs.addTo(c, 1)
    }
    val it = cs.object2IntEntrySet().fastIterator()
    while(it.hasNext) {
      val kv = it.next()
      if (kv.getIntValue > categories.getOrElse(kv.getKey, 0)) {
        return false
      }
    }
    true
  }

  override def isIndependent(elements: Seq[T]): Boolean = {
    PerformanceMetrics.matroidOracleCounterInc()
    val counts = mutable.Map[String, Int]()
    for (k <- categories.keys) {
      counts.put(k, 0)
    }
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

  override def coreSetPoints(elements: Seq[T], k: Int): Seq[T] = independentSetOfSize(elements, k)

}

class TransversalMatroid[T:ClassTag, S](val sets: Array[S],
                                        val getSets: T => Seq[S]) extends Matroid[T] {

  override def isIndependent(elements: Seq[T]): Boolean = {
    PerformanceMetrics.matroidOracleCounterInc()
    if (elements.length > sets.length) {
      return false
    }
    // Return true only if all the elements in the given set are matched to some set
    maximumMatching(elements.toVector)._1 == elements.length
  }


  override def isIndependent(elements: IndexedSubset[T]): Boolean = {
    PerformanceMetrics.matroidOracleCounterInc()
    val arr = Array.ofDim[T](elements.size)
    var i = 0
    var j = 0
    val n = elements.superSet.size
    while (i<n) {
      if (elements.contains(i)) {
        arr(j) = elements.superSet(i)
        j += 1
      }
      i+=1
    }
    isIndependent(arr)
  }

  override def coreSetPoints(elements: Seq[T], k: Int): Seq[T] = {
    val elementsArr = elements.toArray.sortBy(e => -getSets(e).size)
    // First, get an independent set of size k
    val is = independentSetOfSize(elementsArr, k)
    if (is.size == k) {
      println("Independent set of size k!")
      is.toVector
    } else {
      val numAdditionalPoints = is.size
      val output = IndexedSubset(elementsArr)
      val matchedSets: Set[S] = is.toSet.flatMap(x => getSets(x))
      val matchedSetsCounts = mutable.HashMap[S, Int]()
      for (eIdx <- is.supersetIndices; set <- getSets(elements(eIdx))) {
        if (matchedSets.contains(set) && matchedSetsCounts.getOrElse(set, 0) < numAdditionalPoints) {
          output.add(eIdx)
          matchedSetsCounts.update(set, matchedSetsCounts.getOrElse(set, 0) + 1)
        }
      }
      val outVec = output.toVector
      println(s"Independent set smaller than k (cluster of ${elements.size} points)! Selected ${outVec.size} points")
      outVec
    }
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
