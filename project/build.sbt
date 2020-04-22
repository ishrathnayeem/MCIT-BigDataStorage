name := "project"

version := "0.1"

scalaVersion := "2.11.0"
val hadoopVersion = "2.7.3"

libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
