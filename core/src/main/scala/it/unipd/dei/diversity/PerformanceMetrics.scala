package it.unipd.dei.diversity

import com.codahale.metrics.MetricRegistry

object PerformanceMetrics {

  val registry = new MetricRegistry

  val distanceFnCounter = registry.counter("distance-counter")

  val matroidOracleCounter = registry.counter("matroid-oracle")

  def reset(): Unit = {
    distanceFnCounter.dec(distanceFnCounter.getCount)
    matroidOracleCounter.dec(matroidOracleCounter.getCount)
  }

  def distanceFnCounterInc() = distanceFnCounter.inc()

  def matroidOracleCounterInc() = matroidOracleCounter.inc()

}
