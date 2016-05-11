lazy val commonSettings = Seq(
  organization := "it.unipd.dei",
  version := "0.1.0",
  scalaVersion := "2.11.7",
  libraryDependencies ++= Seq(
    "org.scalacheck" %% "scalacheck" % "1.13.0" % "test"
  )
)


lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "diversity-maximization"
  )