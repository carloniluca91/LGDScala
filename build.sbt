val sparkVersion = "2.2.3"

lazy val lgdScala = (project in file("."))
  .settings(

    name := "lgd_scala",
    version := "1.0",
    scalaVersion := "2.11.8",
    scalacOptions ++= Seq(

      "-encoding", "UTF-8"
    ),

    libraryDependencies += ("org.apache.spark" %% "spark-core" % sparkVersion % "provided"),
    libraryDependencies += ("org.apache.spark" %% "spark-sql" % sparkVersion % "provided"),
    libraryDependencies += ("com.github.scopt" %% "scopt" % "3.3.0"),

    assemblyJarName in assembly := s"${name.value}-${version.value}.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x) }
  )