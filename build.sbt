name := "PSOClustering"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core" % "2.1.0",
  "org.apache.spark"  %% "spark-mllib" % "2.1.0",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)

