package course3.project.caseClasses

case class Route(route_id:Option[Int]=None,
                 agency_id:Option[String]=None,
                 route_short_name: Option[String]=None,
                 route_long_name:Option[String]=None,
                 route_type:Option[String]=None,
                 route_url:Option[String]=None,
                 route_color:Option[String]=None)

