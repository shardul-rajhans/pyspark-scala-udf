ThisBuild / version := "1.0.0"

ThisBuild / scalaVersion := "2.12.10"

publishMavenStyle := true

lazy val root = (project in file("."))
  .settings(
    name := "ScalaUDF",
    idePackagePrefix := Some("com.example.udf")
  )
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"  % "provided"
