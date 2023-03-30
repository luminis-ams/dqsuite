addCommandAlias(
  "sanity",
  "compile ;package"
)

val projectName = "dataquality-workflow"
val glueVersion = "4.0.0"
val sparkVersion = "3.3.0"
val scalaVersion_ = "2.12.15"
val awsSdkVersion = "2.20.32"

ThisBuild / version := "0.1"
ThisBuild / scalaVersion := scalaVersion_

lazy val root = (project in file("."))
  .settings(
    name := projectName,
    settings,
    libraryDependencies ++= commonDependencies
  )

lazy val settings = Seq(
  resolvers ++=
    Resolver.sonatypeOssRepos("releases") ++ Seq(
      "aws-glue-etl-artifacts" at "https://aws-glue-etl-artifacts.s3.amazonaws.com/release/",
    )
)

lazy val commonDependencies = Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion_,
  "com.amazonaws" % "AWSGlueETL" % glueVersion % Provided,
  "com.amazon.deequ" % "deequ" % "2.0.3-spark-3.3" % Provided,
  "software.amazon.awssdk" % "timestreamwrite" % awsSdkVersion,
  "software.amazon.awssdk" % "timestreamquery" % awsSdkVersion,
)