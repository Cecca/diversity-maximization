package it.unipd.dei.diversity

import org.rogach.scallop.ScallopConf

class PointsExperimentConf(args: Array[String]) extends ScallopConf(args) {

  lazy val source = opt[String](default = Some("random-gaussian-sphere"))

  lazy val spaceDimension = opt[String](default = Some("2"))

  lazy val delegates = opt[String](required = true)

  lazy val kernelSize = opt[String](required = true)

  lazy val numPoints = opt[String](required = true)

}
