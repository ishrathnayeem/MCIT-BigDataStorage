package course3.project.FileWriterClass

import java.io.OutputStreamWriter
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import au.com.bytecode.opencsv.CSVWriter
import course3.project.caseClasses.EnrichTrip
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}//OK

class ReadandWrite(enrichedList: List[EnrichTrip]) {
  val conf = new Configuration()

  conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  val hadoop:FileSystem = FileSystem.get(conf)
  val out = new OutputStreamWriter( hadoop.create( new Path( "/user/fall2019/ishrath/course3project/finalOutput.csv"), false ) )

  val csvWriter = new CSVWriter(out)
  val csvHeaderFields = Array("Route Id", "Service Id", "Trip Id", "Trip Head Sign", "Direction Id",
    "Shape Id", "Wheelchair accessible", "Note_FR", "Note En", "Agency Id",
    "Route Short Name", "Route Long Name", "Route Type", "Route Url", "Route Colour",
    "Monday", "Tuesday", "Wednesday", "Thrusday", "Friday", "Saturday", "Sunday",
    "Start Date", "End Date")

  var listOfRecords = new ListBuffer[Array[String]]()
  //var listOfRecords:ListBuffer(Array(String)) = new ListBuffer(Array(""))//()
  listOfRecords += csvHeaderFields

  for{element <- enrichedList} yield listOfRecords += Array(
    element.tripRoute.routes.route_id.getOrElse().toString,
    element.calender.service_id.getOrElse().toString,
    element.tripRoute.trips.trip_id.getOrElse().toString, element.tripRoute.trips.trip_headsign.getOrElse().toString,
    element.tripRoute.trips.direction_id.getOrElse().toString, element.tripRoute.trips.shape_id.getOrElse().toString,
    element.tripRoute.trips.wheelchair_accessible.getOrElse().toString, element.tripRoute.trips.note_fr.getOrElse().toString,
    element.tripRoute.trips.note_en.getOrElse().toString, element.tripRoute.routes.agency_id.getOrElse().toString,
    element.tripRoute.routes.route_short_name.getOrElse().toString, element.tripRoute.routes.route_long_name.getOrElse().toString,
    element.tripRoute.routes.route_type.getOrElse().toString, element.tripRoute.routes.route_url.getOrElse().toString,
    element.tripRoute.routes.route_color.getOrElse().toString, element.calender.monday.getOrElse().toString,
    element.calender.tuesday.getOrElse().toString, element.calender.wednesday.getOrElse().toString,
    element.calender.thursday.getOrElse().toString, element.calender.friday.getOrElse().toString,
    element.calender.saturday.getOrElse().toString, element.calender.sunday.getOrElse().toString,
    element.calender.start_date.getOrElse().toString, element.calender.end_date.getOrElse().toString)

  csvWriter.writeAll(listOfRecords.asJava)
  out.close()
}
