package hadoop


import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, FileUtil, Path}

import scala.io.{BufferedSource, Source}

object Main extends App {
  val conf = new Configuration()
  val uri = "/user/fall2019/ishrath/"
  conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  val fs: FileSystem = FileSystem.get(conf)
  println(fs.getUri)

  // ------- Finding the folder----------
  try {
    fs
      .listStatus(new Path("/user/fall2019/ishrath/"))
      .foreach(println)
    println("I found my File")
  }
  catch{
    case f : FileNotFoundException =>
      println("file not found")
  }

  // -------- Deleting the folder ---------
  try {
    fs
      .delete (new Path("/user/fall2019/ishrath"),true)
    fs
      .listStatus(new Path("/user/fall2019/ishrath"))
    println("Directory not deleted")
  }
  catch{
    case f : FileNotFoundException =>
      println("Directory was deleted")
  }

  //------------- Making a new Folder ------
  try {
    fs
      .mkdirs(new Path("/user/fall2019/ishrath"))
    fs
      .listStatus(new Path("/user/fall2019/ishrath"))
    println("Folder Created")
  }

  try {
    fs
      .mkdirs(new Path("/user/fall2019/ishrath/assignment"))
  }

  catch{
    case f : FileNotFoundException =>
      println("Folder not created")
  }

  // -------- Making a sub Folder -----------
  try {
    fs
      .mkdirs(new Path("/user/fall2019/ishrath/assignment/stm"))
    fs
      .listStatus(new Path("/user/fall2019/ishrath/assignment/stm"))
    println("SubFolder Created")
  }
  catch{
    case f : FileNotFoundException =>
      println("SubFolder not Created")
  }

  // -------Uploading files from the Local to the Cluster-------
  try {
    fs
      .copyFromLocalFile(new Path("/Users/ishrathnayeem/MY MAC/Study/Big Data/MCIT/2. Functions of Big Data/Finalised Project/gtfs_stm/stops.txt"),
        new Path("/user/fall2019/ishrath/assignment/stm/stops.txt"))
  }

  // ------- Duplicating the file into the same folder ---------

  val sourcePath = new Path("/user/fall2019/ishrath/assignment/stm/stops.txt")
  val destinationPath = new Path("/user/fall2019/ishrath/assignment/stm/stops2.txt")
  FileUtil.copy (sourcePath.getFileSystem(conf),sourcePath,destinationPath.getFileSystem(conf),destinationPath,false,conf)
  println("Sucessfully copied")
  println(fs.resolvePath(new Path("/user/fall2019/ishrath/assignment/stm/stops2.txt")))


  // -------- Renaming the file -----------
  try {
    fs
      .rename(new Path("/user/fall2019/ishrath/assignment/stm/stops2.txt"), new Path("/user/fall2019/ishrath/assignment/stm/stops.csv"))
  }

  // -------Printing first 5 lines of the file ---------
  val filePath = new Path("/user/fall2019/ishrath/assignment/stm/stops.csv")
  val stream: FSDataInputStream = fs.open(filePath)
  val source: BufferedSource = Source.fromInputStream(stream)
  printf(" ")
  source.getLines().slice(0,5).foreach(println)

}
