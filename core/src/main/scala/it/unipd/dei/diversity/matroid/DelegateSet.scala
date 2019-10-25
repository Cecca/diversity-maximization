package it.unipd.dei.diversity.matroid

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Allows to incrementally accumulate the points that are in each cluster
  * while building the coreset. Needed mainly by the streaming algorithm
  */
trait DelegateSet[T] { self =>
  def add(point: T): Boolean
  def merge(other: DelegateSet[T]): DelegateSet[T]
  def toSeq: Seq[T]
}

class UniformDelegateSet[T](val k: Int, val inner: ArrayBuffer[T]) extends DelegateSet[T] {
  inner.sizeHint(k)

  override def add(point: T): Boolean = {
    if (inner.length == k) return false
    inner.append(point)
    true
  }

  override def merge(other: DelegateSet[T]): DelegateSet[T] = other match {
    case other: UniformDelegateSet[T] => new UniformDelegateSet[T](this.k, (this.inner ++ other.inner).take(k))
    case _ => throw new RuntimeException("Unsupported merge")
  }

  override def toSeq: Seq[T] = inner.toVector
}

class PartitionDelegateSet[T](val k: Int,
                              matroid: PartitionMatroid[T],
                              val inner: mutable.HashMap[String, ArrayBuffer[T]]) extends DelegateSet[T] {
  private var _frozen = false

  override def add(point: T): Boolean = {
    if (_frozen || matroid.hasIndependentSetOfSize(inner.values.flatten.toSeq, k)) {
      _frozen = true
      return true
    }
    val cat = matroid.getCategory(point)
    val set = inner(cat)
    if (set.size < matroid.categories(cat)) {
      set.append(point)
      true
    } else {
      false
    }
  }

  override def merge(other: DelegateSet[T]): DelegateSet[T] = other match {
    case other: PartitionDelegateSet[T] =>
      val inner = new mutable.HashMap[String, ArrayBuffer[T]]()
      for (cat <- this.matroid.categories.keys) {
        val set = new ArrayBuffer[T]()
        set.appendAll(
          (this.inner.getOrElse(cat, ArrayBuffer.empty).iterator ++
          other.inner.getOrElse(cat, ArrayBuffer.empty)).take(matroid.categories(cat))
        )
        inner(cat) = set
      }
      println(s"After the merge, delegate set of size ${inner.size} (before ${this.inner.size}, ${other.inner.size})")
      new PartitionDelegateSet[T](k, matroid, inner)
    case _ => throw new RuntimeException("Unsupported merge")
  }

  override def toSeq: Seq[T] = matroid.coreSetPoints(inner.values.flatten.toSeq, k)
}

class TransversalDelegateSet[T, S](val k: Int,
                                   val matroid: TransversalMatroid[T, S],
                                   val inner: mutable.HashMap[S, ArrayBuffer[T]]) extends DelegateSet[T] {

  private var _frozen = false

  override def toString: String = inner.values.flatten.mkString("{", ", ", "}")

  override def add(point: T): Boolean = {
    if (_frozen || matroid.hasIndependentSetOfSize(inner.values.flatten.toSeq, k)) {
      _frozen = true
      return true
    }
    for (s <- matroid.getSets(point)) {
      if (inner(s).size < k) {
        inner(s).append(point)
        return true
      }
    }
    false
  }

  override def merge(other: DelegateSet[T]): DelegateSet[T] = other match {
    case other: TransversalDelegateSet[T, S] =>
      val inner = new mutable.HashMap[S, ArrayBuffer[T]]()
      for (cat <- this.matroid.sets) {
        val set = new ArrayBuffer[T]()
        set.appendAll(
          (this.inner.getOrElse(cat, ArrayBuffer.empty).iterator ++
          other.inner.getOrElse(cat, ArrayBuffer.empty).iterator).take(k)
        )
        inner(cat) = set
      }
      val ret = new TransversalDelegateSet[T, S](k, matroid, inner)
      ret
    case _ => throw new RuntimeException("Unsupported merge")
  }

  override def toSeq: Seq[T] = {
    println(s"Categories cardinalities (there are ${inner.size} categories) ${inner.values.map(_.size).mkString(", ")}")
    val delegates = inner.values.flatten.toSeq
//    val is = matroid.independentSetOfSize(delegates, k)
//    if (is.length == k) {
//      matroid.coreSetPoints(delegates, k)
//    } else {
//      delegates
//    }
    delegates
  }
}
