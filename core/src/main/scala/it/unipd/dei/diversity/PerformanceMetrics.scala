package it.unipd.dei.diversity

import com.codahale.metrics.MetricRegistry

object PerformanceMetrics {

  val registry = new MetricRegistry

  val distanceFnCounter = registry.counter("distance-counter")

  val matroidOracleCounter = registry.counter("matroid-oracle")

  def distanceFnCounterInc() = distanceFnCounter.inc()

  def matroidOracleCounterInc() = matroidOracleCounter.inc()

}
