package it.unipd.dei.diversity

import org.roaringbitmap.RoaringBitmap
import org.scalameter.api._

import scala.collection.BitSet
import scala.collection.immutable.HashSet
import scala.util.Random

object DistanceBenchmark extends Bench.OfflineReport {

  val vocabulary: Vector[Int] = (0 until 10000).toVector

  val sizes = Gen.exponential("size")(8, 8192, 2)
  val simplePairs = for {
    size <- sizes
  } yield (SimpleBOW(vocabulary, size), SimpleBOW(vocabulary, size))

  val hashSetPairs = for {
    size <- sizes
  } yield (HashSetBOW(vocabulary, size), HashSetBOW(vocabulary, size))

  val bitsetPairs = for {
    size <- sizes
  } yield (BitSetBOW(vocabulary, size), BitSetBOW(vocabulary, size))

  val roaringPairs = for {
    size <- sizes
  } yield (RoaringBOW(vocabulary, size), RoaringBOW(vocabulary, size))

  val intPairs = for {
    size <- sizes
  } yield (IntBOW(vocabulary, size), IntBOW(vocabulary, size))


  performance of "Distance.euclidean" in {

    measure method "SimpleBOW" in {
      using(simplePairs) in { case (a, b) =>
        Distance.euclidean(a, b)
      }
    }

    measure method "HashSetBOW" in {
      using(hashSetPairs) in { case (a, b) =>
        Distance.euclidean(a, b)
      }
    }

    measure method "BitSetBOW" in {
      using(bitsetPairs) in { case (a, b) =>
        Distance.euclidean(a, b)
      }
    }

    measure method "RoaringBOW" in {
      using(roaringPairs) in { case (a, b) =>
        Distance.euclidean(a, b)
      }
    }

    measure method "IntBOW" in {
      using(intPairs) in { case (a, b) =>
        Distance.euclidean(a, b)
      }
    }

  }

}

class SimpleBOW(override val wordCounts: Map[Int, Int]) extends BagOfWords[Int]
object SimpleBOW {
  def apply(vocabulary: Vector[Int], size: Int): SimpleBOW ={
    val wc = Random.shuffle(vocabulary).take(size).map { w =>
      (w, Random.nextInt())
    }.toMap
    new SimpleBOW(wc)
  }
}

class HashSetBOW(override val wordCounts: Map[Int, Int]) extends BagOfWords[Int] {
  override val words: Set[Int] = HashSet(wordCounts.keys.toSeq :_*).asInstanceOf[Set[Int]]
}
object HashSetBOW {
  def apply(vocabulary: Vector[Int], size: Int): HashSetBOW ={
    val wc = Random.shuffle(vocabulary).take(size).map { w =>
      (w, Random.nextInt())
    }.toMap
    new HashSetBOW(wc)
  }
}

class BitSetBOW(override val wordCounts: Map[Int, Int]) extends BagOfWords[Int] {
  override val words: Set[Int] =
    BitSet(wordCounts.keys.toSeq :_*).asInstanceOf[Set[Int]]
}
object BitSetBOW {
  def apply(vocabulary: Vector[Int], size: Int): BitSetBOW ={
    val wc = Random.shuffle(vocabulary).take(size).map { w =>
      (w, Random.nextInt())
    }.toMap
    new BitSetBOW(wc)
  }
}

class RoaringBOW(override val wordCounts: Map[Int, Int]) extends BagOfWords[Int]{

  private val roaringWords = RoaringBitmap.bitmapOf(wordCounts.keys.toSeq :_*)

  override def wordUnion(other: BagOfWords[Int]): Iterator[Int] = {
    val otherRoaringWords = other.asInstanceOf[RoaringBOW].roaringWords
    val union = RoaringBitmap.or(this.roaringWords, otherRoaringWords)
    new Iterator[Int] {
      val it = union.getIntIterator
      override def hasNext: Boolean = it.hasNext
      override def next(): Int = it.next()
    }
  }
}
object RoaringBOW {
  def apply(vocabulary: Vector[Int], size: Int): RoaringBOW ={
    val wc = Random.shuffle(vocabulary).take(size).map { w =>
      (w, Random.nextInt())
    }.toMap
    new RoaringBOW(wc)
  }
}

object IntBOW {
  def apply(vocabulary: Vector[Int], size: Int): IntBagOfWords ={
    val wc = Random.shuffle(vocabulary).take(size).map { w =>
      (w, Random.nextInt())
    }.toMap
    new IntBagOfWords {
      override def wordCounts: Map[Int, Int] = wc
    }
  }

}