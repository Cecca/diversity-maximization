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

lazy val root = (project in file(".")).
  aggregate(core).
  settings(baseSettings :_*)

/** Configuration for benchmarks */
lazy val Benchmark = config("bench") extend Test

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    name := "diversity-maximization-core",
    libraryDependencies ++= Seq(
      "com.storm-enroute" %% "scalameter" % "0.7" % "bench",
      "org.roaringbitmap" % "RoaringBitmap" % "0.5.11",
      "io.dropwizard.metrics" % "metrics-core" % "3.1.2",
      "it.unimi.dsi" % "dsiutils" % "2.3.2" exclude("ch.qos.logback", "logback-classic")
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
      "it.unipd.dei" % "experiment-reporter" % "0.3.0",
      "org.rogach" %% "scallop" % "1.0.1",
      "com.typesafe.akka" %% "akka-stream" % "2.4.5" % "provided",
      "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
      "com.storm-enroute" %% "scalameter" % "0.7" % "bench"
    ),
    deploy := deployTaskImpl.value
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
  settings(inConfig(Benchmark)(Defaults.testSettings): _*)

//////////////////////////////////////////////////////////////////////////////
// Custom tasks

lazy val deploy = Def.taskKey[Unit]("Deploy the jar")

lazy val deployTaskImpl = Def.task {
  val log = streams.value.log
  val account = "ceccarel@stargate.dei.unipd.it"
  val local = assembly.value.getPath
  val fname = assembly.value.getName
  val remote = s"$account:/mnt/gluster/ceccarel/lib/$fname"
  log.info(s"Deploy $fname to $remote")
  Seq("rsync", "--progress", "-z", local, remote) !
}
