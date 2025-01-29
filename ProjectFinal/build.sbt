ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "ProjectFinal"
  )
libraryDependencies+= "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies+= "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies+= "log4j" % "log4j" % "1.2.17"

