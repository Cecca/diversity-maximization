lazy val commonSettings = Seq(
  organization := "it.unipd.dei",
  version := "0.1.0",
  scalaVersion := "2.11.8",
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "2.2.6" % "test",
    "org.scalacheck" %% "scalacheck" % "1.13.0" % "test"
  )
)

lazy val root = (project in file(".")).
  aggregate(core)

/** Configuration for benchmarks */
lazy val Benchmark = config("bench") extend Test

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    name := "diversity-maximization-core",
    libraryDependencies ++= Seq(
      "com.storm-enroute" %% "scalameter" % "0.7"
    ),
    testFrameworks in Benchmark += new TestFramework("org.scalameter.ScalaMeterFramework"),
    parallelExecution in Benchmark := false,
    logBuffered in Benchmark := false
  ).
  configs(Benchmark).
  settings(inConfig(Benchmark)(Defaults.testSettings): _*)

lazy val experiments = (project in file("experiments")).
  dependsOn(core).
  settings(commonSettings :_*).
  settings(
    name := "diversity-maximization-experiments",
    libraryDependencies ++= Seq(
      "it.unipd.dei" % "experiment-reporter" % "0.2.0",
      "org.rogach" %% "scallop" % "1.0.1",
      "it.unimi.dsi" % "dsiutils" % "2.3.2",
      "com.typesafe.akka" %% "akka-stream" % "2.4.5",
      "io.dropwizard.metrics" % "metrics-core" % "3.1.0"
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
