package it.unipd.dei.diversity

/**
  * A mutable view of a subset of a set represented by and indexed seq.
  */
class IndexedSubset[T] private (val superSet: IndexedSeq[T], private val flags: Array[Boolean]) {

  def add(idx: Int): Unit = flags(idx) = true

  def remove(idx: Int): Unit = flags(idx) = false

  def contains(idx: Int): Boolean = flags(idx)

  def toSet: Set[T] =
    superSet.zipWithIndex.filter({case (e, i) => flags(i)}).map(_._1).toSet

  def copy(): IndexedSubset[T] = new IndexedSubset[T](superSet, flags.clone())

}

object IndexedSubset {

  def apply[T](superSet: IndexedSeq[T]) = new IndexedSubset[T](superSet, Array.ofDim(superSet.size))

}