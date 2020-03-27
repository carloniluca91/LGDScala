ThisBuild / organization := "it.carloni.luca"
ThisBuild / name := "lgd-scala"
ThisBuild / scalaVersion := "2.11.0"
ThisBuild / version := "0.1"

val sparkVersion = "2.2.3"

assemblyJarName in assembly := s"$name-$version.jar"

lazy val lgd_scala = (project in file("."))
  .settings(
    libraryDependencies += ("org.apache.spark" %% "spark-core" % sparkVersion),
    libraryDependencies += ("org.apache.spark" %% "spark-sql" % sparkVersion),
    libraryDependencies += ("com.github.scopt" %% "scopt" % "3.3.0")
  )

