name := "streamDM (Structured Streaming)"

version := "0.2"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.1"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.3"
