package course3.project.configuration

  import java.io.BufferedInputStream
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.{FileSystem, Path}

  trait Config { //This trait is used for other method

    val conf = new Configuration()

    conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
    conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
    val hadoop: FileSystem = FileSystem.get(conf)

    val tripfile = new BufferedInputStream(hadoop.open(new Path("/user/fall2019/ishrath/stm/trips.txt")))
    val routefile = new BufferedInputStream(hadoop.open(new Path("/user/fall2019/ishrath/stm/routes.txt")))
    val calendarfile = new BufferedInputStream(hadoop.open(new Path("/user/fall2019/ishrath/stm/calendar.txt")))

  }




