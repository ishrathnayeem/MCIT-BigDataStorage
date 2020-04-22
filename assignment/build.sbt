name := "assignment"

version := "0.1"

scalaVersion := "2.11.0"

val hadoopVersion = "2.7.3"

//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
//libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion

libraryDependencies ++= Seq (
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-hdfs",
).map( _ % hadoopVersion)
