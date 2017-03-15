package it.unipd.dei.diversity.matroid

import scala.collection.mutable

trait Matroid[T] {

  def isIndependent(elements: Seq[T]): Boolean

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

}

object TransversalMatroid {

  @inline def indexOf(sets: Array[Int], s: Int): Int = java.util.Arrays.binarySearch(sets, s)

  def hasAugmentingPath(matched: Array[Int],
                        sets: Array[Int],
                        elements: Array[Array[Int]],
                        element: Int,
                        seen: Array[Boolean]): Boolean = {
    // Try all the jobs adjacent to the given element
    for (s <- elements(element)) {
      val sIdx = indexOf(sets, s)
      if (!seen(sIdx)) {
        seen(sIdx) = true

        if (matched(sIdx) < 0 || hasAugmentingPath(matched, sets, elements, matched(sIdx), seen)) {
          matched(sIdx) = element
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
    for (e <- elements.indices) {
      for (i <- seen.indices) {
        seen(i) = false
      }
      if (hasAugmentingPath(matched, sets, elements, e, seen)) {
        matchedCnt += 1
      }
    }

    matchedCnt
  }

}
