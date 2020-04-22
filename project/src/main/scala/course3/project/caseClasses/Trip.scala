package course3.project.caseClasses

case class Trip(route_id:Option[Int]=None,
                service_id:Option[String]=None,
                trip_id:Option[String]=None,
                trip_headsign:Option[String]=None,
                direction_id:Option[String]=None,
                shape_id:Option[String]=None,
                wheelchair_accessible:Option[String]=None,
                note_fr:Option[String]=None,
                note_en:Option[String] =None)
