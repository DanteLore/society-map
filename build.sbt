
name := "society-map"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % "2.11.8",
  "log4j" % "log4j" % "1.2.14",
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.0.0",
  "com.github.nscala-time" %% "nscala-time" % "2.14.0",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test"
)

