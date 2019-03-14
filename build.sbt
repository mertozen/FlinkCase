ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "flink-project"

version := "0.1-SNAPSHOT"

organization := "com.mert"

ThisBuild / scalaVersion := "2.12.8"

val flinkVersion = "1.7.2"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime,
  "org.apache.flink" % "flink-test-utils-junit" % flinkVersion,
  "org.apache.flink" %% "flink-table" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

assembly / mainClass := Some("com.mert.ClickDataMain")

// make run command include the provided dependencies
Compile / run := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
