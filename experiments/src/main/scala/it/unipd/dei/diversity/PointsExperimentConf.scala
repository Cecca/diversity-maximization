package it.unipd.dei.diversity

import org.rogach.scallop.ScallopConf

class PointsExperimentConf(args: Array[String]) extends ScallopConf(args) {

  lazy val source = opt[String](default = Some("versor"))

  lazy val spaceDimension = opt[String](default = Some("2"))

  lazy val delegates = opt[String](required = true)

  lazy val kernelSize = opt[String](required = true)

  lazy val numPoints = opt[String](required = true)

  lazy val runs = opt[Int](default = Some(1))

  lazy val farthest = toggle(
    default=Some(true),
    descrYes = "Compute metrics based on the farthest-point heuristic",
    descrNo  = "Don't compute metrics based on the farthest-point heuristic")

  lazy val matching = toggle(
    default=Some(true),
    descrYes = "Compute metrics based on the matching heuristic",
    descrNo  = "Don't compute metrics based on the matching heuristic")

}
