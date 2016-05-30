package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.Point

/**
  * Thin wrapper around [[Iterator]] that provides serialization
  */
abstract class RandomPointIterator extends Iterator[Point] with Serializable {

  override def hasNext: Boolean = true

}
