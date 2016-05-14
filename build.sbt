lazy val commonSettings = Seq(
  organization := "it.unipd.dei",
  version := "0.1.0",
  scalaVersion := "2.11.7",
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "2.2.6" % "test",
    "org.scalacheck" %% "scalacheck" % "1.13.0" % "test"
  )
)

lazy val root = (project in file(".")).
  aggregate(core)

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    name := "diversity-maximization-core"
  )

lazy val experiments = (project in file("experiments")).
  dependsOn(core).
  settings(commonSettings :_*).
  settings(
    name := "diversity-maximization-experiments",
    libraryDependencies ++= Seq(
      "it.unipd.dei" % "experiment-reporter" % "0.2.0",
      "org.rogach" %% "scallop" % "1.0.1"
    )
  ).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](
      version,
      "gitRevision" -> "git rev-parse HEAD".!!.trim,
      "gitRevCount" -> "git log --oneline".!!.split("\n").length
    ),
    buildInfoPackage := "it.unipd.dei.diversity"
  )
