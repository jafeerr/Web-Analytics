package com.mindtree.Insight.Entities

import java.sql.Date

case class DailyInsight(date:Date,timeid:java.util.UUID,noofuniquevisitors:Long,noofsessions:Long,averagesessionduration:Double,totalnoofpageviews:Long,
                        popularpages:Map[String,Long],pageswithhigherexitrates:Map[String,Long],bouncerate:Long,bounceratepercent:Double,
                        trafficbycountries:Map[String,Double],newusers:Long)

