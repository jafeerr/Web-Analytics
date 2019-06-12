package com.mindtree.entities

case class Request(hostName: String, timestamp: String, ipValue:Long, httpMethod: String, uri: String,responseCode: Integer,
                   bytes: Integer)

case class PathDetails(hdfsPath:String,hdfsFileName:String,ipCountryMapFilePath:String,ipCountryMapFileName:String,hivePath:String)

case class IPCountryMapping(startIp:Long,endIp:Long,countryCode:String,countryName:String)
