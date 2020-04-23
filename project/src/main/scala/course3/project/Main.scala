package course3.project

import java.io.BufferedInputStream
import course3.project.fileWriterClass.ReadAndWrite
import course3.project.caseClasses._
import course3.project.lookupClasses._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable.ListBuffer
import scala.io.Source

//object Main extends App with Config {           // Used for another method

   object Main extends App {

     val conf = new Configuration()

     conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
     conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
     val hadoop: FileSystem = FileSystem.get(conf)

     val tripfile = new BufferedInputStream(hadoop.open(new Path("/user/fall2019/ishrath/stm/trips.txt")))
     val routefile = new BufferedInputStream(hadoop.open(new Path("/user/fall2019/ishrath/stm/routes.txt")))
     val calendarfile = new BufferedInputStream(hadoop.open(new Path("/user/fall2019/ishrath/stm/calendar.txt")))

     val tripList = Source.fromInputStream(tripfile).getLines().drop(1)
       .map(line => line.split(","))
       .map(a => Trip(Some(a(0).toInt), Some(a(1)), Some(a(2)), Some(a(3)), Some(a(4)), Some(a(5)), Some(a(6)))).toList

     val routeList = Source.fromInputStream(routefile).getLines().drop(1)
       .map(line => line.split(","))
       .map(a => Route(Some(a(0).toInt), Some(a(1)), Some(a(2)), Some(a(3)), Some(a(4)), Some(a(5)), Some(a(6)))).toList

     val calenderList = Source.fromInputStream(calendarfile).getLines().drop(1)
       .map(line => line.split(","))
       .map(a => Calender(Some(a(0)), Some(a(1)), Some(a(2)), Some(a(3)), Some(a(4)), Some(a(5)), Some(a(6)), Some(a(7)),
         Some(a(8)), Some(a(9)))).toList

     val routeLookup = new RouteLookup(routeList)
     val calenderLookUp = new CalendarLookup(calenderList)

     val enrichedTripRouteResult: List[TripRoute] = tripList.map(trip => TripRoute(trip,
       routeLookup.lookup(trip.route_id.getOrElse(0))))

     var enrichedTripResult = new ListBuffer[EnrichTrip]()
     for {
       tripRoute <- enrichedTripRouteResult
     } yield enrichedTripResult += EnrichTrip(tripRoute, calenderLookUp.lookup(tripRoute.trips.service_id.getOrElse("")))

     //FIltering the Enriched Data
     val writer = new ReadAndWrite(enrichedTripResult.toList.filter(p => p.calender.monday.getOrElse().toString == "1"
       && p.tripRoute.routes.route_url.getOrElse().toString.contains("metro")))

   }
