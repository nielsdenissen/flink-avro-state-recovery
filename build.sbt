import scalariform.formatter.preferences._

name := "flink-avro"

version := "0"

scalaVersion in ThisBuild := "2.11.11"

scalacOptions := Seq(
  "-encoding", "utf8",
  "-target:jvm-1.8",
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-unchecked",
  "-deprecation",
  "-Xlog-reflective-calls"
)

val flinkVersion = "1.4.1"

resolvers ++= Seq(
  Resolver.bintrayRepo("cakesolutions", "maven"),
  Resolver.jcenterRepo
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.avro"         %    "avro"                            % "1.8.2",
      "org.apache.flink"        %%   "flink-streaming-scala"           % flinkVersion,
      "org.apache.flink"        %%   "flink-streaming-contrib"         % flinkVersion  % Test,
      "ch.qos.logback"          %    "logback-classic"                 % "1.1.3",
      "org.scalatest"           %%   "scalatest"                       % "3.0.1"       % Test
    )
  )

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 100)
  .setPreference(DoubleIndentClassDeclaration, true)

cancelable in Global := true

fork in run := true

//Coverage settings
coverageMinimum := 70
coverageFailOnMinimum := false
coverageHighlighting := true
coverageExcludedPackages := ".*Tables.*"

publishArtifact in Test := true

parallelExecution in Test := false
parallelExecution in IntegrationTest := false

run in Compile := Defaults.runTask(
  fullClasspath in Compile,
  mainClass in (Compile, run),
  runner in (Compile,run)
).evaluated

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case _ => MergeStrategy.first
}
