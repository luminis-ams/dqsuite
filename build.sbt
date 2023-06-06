addCommandAlias(
  "sanity",
  "compile ;assembly"
)
lazy val copyJarsTask = taskKey[Unit]("Copy required jars to the lib folder")

val projectName = "dqsuite"
val glueVersion = "4.0.0"
val sparkVersion = "3.3.0"
val scalaCompatVersion = "2.12"
val scalaVersion_ = s"$scalaCompatVersion.15"
val awsSdkVersion = "2.20.32"
val awsJavaSdkVersion = "1.12.128"

ThisBuild / version := "0.2.0"
ThisBuild / scalaVersion := scalaVersion_

lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .aggregate(
    core,
    examples,
  )

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    assemblyPackageScala / assembleArtifact := false,
    assembly / assemblyJarName := s"$projectName-bundle_$scalaCompatVersion-${version.value}.jar",
    assembly / assemblyOutputPath := file(s"out/libs/${(assembly / assemblyJarName).value}"),
    name := projectName,
    libraryDependencies ++= commonDependencies ++ coreDependencies
  )

lazy val examples = (project in file("examples"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    name := "examples",
    commonSettings,
    examplesSettings,
    libraryDependencies ++= commonDependencies ++ examplesDependencies
  )
  .dependsOn(
    core
  )

lazy val commonDependencies = Seq(
  "com.amazon.deequ" % "deequ" % "2.0.3-spark-3.3" % Provided,
)

lazy val coreDependencies = Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion_,
  "org.yaml" % "snakeyaml" % "2.0",
  // Included in AWS Glue 4.0.0
  "software.amazon.awssdk" % "timestreamwrite" % awsSdkVersion % Provided,
  "software.amazon.awssdk" % "cloudwatch" % awsSdkVersion % Provided,
//  "com.amazonaws" % "aws-java-sdk-cloudwatchmetrics" % awsJavaSdkVersion % Provided,
  "com.typesafe" % "config" % "1.3.3" % Provided,
)

lazy val examplesDependencies = Seq(
  "com.amazonaws" % "AWSGlueETL" % glueVersion % Provided,
)

lazy val examplesSettings = Seq(
  resolvers ++=
    Resolver.sonatypeOssRepos("releases") ++ Seq(
      "aws-glue-etl-artifacts" at "https://aws-glue-etl-artifacts.s3.amazonaws.com/release/",
    )
)

lazy val commonSettings = Seq(
  copyJarsTask := {
    val folder = new File("out/libs")

    // Copy dependencies
    (Compile / managedClasspath).value.files
      .filter(_.getName.contains("deequ"))
      .foreach { f =>
        println(s"Copying ${f.getName}")
        IO.copyFile(f, folder / f.getName, CopyOptions().withOverwrite(false))
      }

    // Copy artifacts
    val (_, f) = (Compile / packageBin / packagedArtifact).value
    println(s"Copying artifacts ${f.getName}")
    IO.copyFile(f, folder / f.getName, CopyOptions().withOverwrite(true))
  }
)