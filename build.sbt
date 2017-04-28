

lazy val baseSettings = Seq(
  organization := "it.unipd.dei",
  version := "0.1.0",
  scalaVersion := "2.11.8"
)

lazy val commonSettings = baseSettings ++ Seq(
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "2.2.6" % "test",
    "org.scalacheck" %% "scalacheck" % "1.13.0" % "test"
  ),
  test in assembly := {},
  scalacOptions := Seq(
    "-optimise",
    "-Xdisable-assertions",
    "-feature",
    "-deprecation",
    "-unchecked"))

////////////////////////////////////////////////////////////
// Custom task definition

lazy val deploy = inputKey[Unit]("Deploy the jar to the given ssh host (using rsync)")

def filterDeps(deps: Seq[ModuleID]): Seq[ModuleID] = {
  deps.map({ d =>
    d.exclude("org.slf4j", "slf4j-jdk14")
      .exclude("commons-logging", "commons-logging")
      .exclude("org.slf4j", "slf4j-log4j12")
  })
}

////////////////////////////////////////////////////////////
// Projects

lazy val root = (project in file(".")).
  aggregate(core, mllib, experiments).
  settings(baseSettings :_*)

/** Configuration for benchmarks */
lazy val Benchmark = config("bench") extend Test

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    name := "diversity-maximization-core",
    libraryDependencies ++= filterDeps(Seq(
      "com.storm-enroute" %% "scalameter" % "0.7" % "bench",
      "io.dropwizard.metrics" % "metrics-core" % "3.1.2",
      "it.unimi.dsi" % "dsiutils" % "2.3.2",
      "it.unimi.dsi" % "fastutil" % "7.1.0",
      "ch.qos.logback" % "logback-classic" % "1.1.7"
    )),
    testFrameworks in Benchmark += new TestFramework("org.scalameter.ScalaMeterFramework"),
    parallelExecution in Benchmark := false,
    logBuffered in Benchmark := false
  ).
  configs(Benchmark).
  settings(inConfig(Benchmark)(Defaults.testSettings): _*)

lazy val mllib = (project in file("mllib")).
  dependsOn(core).
  settings(commonSettings: _*).
  settings(
    name := "diversity-maximization-mllib",
    libraryDependencies ++= filterDeps(Seq(
      "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided",
      "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
      "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models"
    ))
  )

lazy val experiments = (project in file("experiments")).
  dependsOn(core, mllib).
  settings(commonSettings :_*).
  settings(
    name := "diversity-maximization-experiments",
    libraryDependencies ++= filterDeps(Seq(
      "it.unipd.dei" % "experiment-reporter" % "0.3.0",
      "org.rogach" %% "scallop" % "1.0.1",
      "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
      "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided",
      "com.storm-enroute" %% "scalameter" % "0.7" % "bench"
    ))
  ).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](
      version,
      "gitBranch"   -> "git rev-parse --abbrev-ref HEAD".!!.trim,
      "gitRevision" -> "git rev-parse HEAD".!!.trim,
      "gitRevCount" -> "git log --oneline".!!.split("\n").length
    ),
    buildInfoPackage := "it.unipd.dei.diversity"
  ).
  configs(Benchmark).
  settings(inConfig(Benchmark)(Defaults.testSettings): _*).
  settings(
    deploy := {
      import sbt._
      import complete.DefaultParsers._

      val arg = spaceDelimited("<user@domain:path>").parsed
      arg.headOption match {
        case None => sys.error("Please provide the remote to which you want to deploy")
        case Some(remote) =>
          val log = streams.value.log
          val local = assembly.value.getPath
          val fname = assembly.value.getName
          log.info(s"Deploy $fname to $remote")
          Seq("rsync", "--progress", "-z", local, remote) !
      }
    }
  )
