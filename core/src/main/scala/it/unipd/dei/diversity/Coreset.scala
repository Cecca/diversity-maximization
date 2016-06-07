package it.unipd.dei.diversity

trait Coreset[T] {

  def kernel: Vector[T]

  def delegates: Vector[T]

  def points: Vector[T] = kernel ++ delegates

}
