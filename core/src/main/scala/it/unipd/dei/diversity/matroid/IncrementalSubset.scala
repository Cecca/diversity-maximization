package it.unipd.dei.diversity.matroid

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Allows to incrementally accumulate the points that are in each cluster
  * while building the coreset. Needed mainly by the streaming algorithm
  */
trait IncrementalSubset[T] { self =>
  def add(point: T): Boolean
  def merge(other: IncrementalSubset[T]): IncrementalSubset[T]
  def toSeq: Seq[T]
}

class UniformIncrementalSubset[T](val k: Int, val inner: ArrayBuffer[T]) extends IncrementalSubset[T] {
  inner.sizeHint(k)

  override def add(point: T): Boolean = {
    if (inner.length == k) return false
    inner.append(point)
    true
  }

  override def merge(other: IncrementalSubset[T]): IncrementalSubset[T] = other match {
    case other: UniformIncrementalSubset[T] => new UniformIncrementalSubset[T](this.k, (this.inner ++ other.inner).take(k))
    case _ => throw new RuntimeException("Unsupported merge")
  }

  override def toSeq: Seq[T] = inner.toVector
}

class PartitionIncrementalSubset[T](val k: Int,
                                    matroid: PartitionMatroid[T],
                                    val inner: mutable.HashMap[String, ArrayBuffer[T]]) extends IncrementalSubset[T] {
  override def add(point: T): Boolean = {
    val cat = matroid.getCategory(point)
    val set = inner(cat)
    if (set.size < matroid.categories(cat)) {
      set.append(point)
      true
    } else {
      false
    }
  }

  override def merge(other: IncrementalSubset[T]): IncrementalSubset[T] = other match {
    case other: PartitionIncrementalSubset[T] =>
      val inner = new mutable.HashMap[String, ArrayBuffer[T]]()
      for (cat <- this.matroid.categories.keys) {
        val set = new ArrayBuffer[T]()
        set.appendAll(
          (this.inner.getOrElse(cat, ArrayBuffer.empty).iterator ++
          other.inner.getOrElse(cat, ArrayBuffer.empty)).take(matroid.categories(cat))
        )
        inner(cat) = set
      }
      new PartitionIncrementalSubset[T](k, matroid, inner)
    case _ => throw new RuntimeException("Unsupported merge")
  }

  override def toSeq: Seq[T] = matroid.coreSetPoints(inner.values.flatten.toSeq, k)
}

class TransversalIncrementalSubset[T, S](val k: Int,
                                         val matroid: TransversalMatroid[T, S],
                                         val inner: mutable.HashMap[S, ArrayBuffer[T]]) extends IncrementalSubset[T] {
  override def add(point: T): Boolean = {
    for (s <- matroid.getSets(point)) {
      if (inner(s).size < k) {
        inner(s).append(point)
        return true
      }
    }
    false
  }

  override def merge(other: IncrementalSubset[T]): IncrementalSubset[T] = other match {
    case other: TransversalIncrementalSubset[T, S] =>
      val inner = new mutable.HashMap[S, ArrayBuffer[T]]()
      for (cat <- this.matroid.sets) {
        val set = new ArrayBuffer[T]()
        set.appendAll(
          (this.inner.getOrElse(cat, ArrayBuffer.empty).iterator ++
          other.inner.getOrElse(cat, ArrayBuffer.empty)).take(k)
        )
        inner(cat) = set
      }
      new TransversalIncrementalSubset[T, S](k, matroid, inner)
    case _ => throw new RuntimeException("Unsupported merge")
  }

  override def toSeq: Seq[T] = matroid.coreSetPoints(inner.values.flatten.toSeq, k)
}