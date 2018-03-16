name := "EvolutionGamingTest"

scalaVersion := "2.11.4"

version := "1.0"

val versions = new Object {
  val spark = "2.2.0"
  val scalaTest = "3.0.5"
}

libraryDependencies += "org.apache.spark" %% "spark-core" % versions.spark
libraryDependencies += "org.apache.spark" %% "spark-sql" % versions.spark
libraryDependencies += "org.scalatest" %% "scalatest" % versions.scalaTest % Test
