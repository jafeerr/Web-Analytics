package com.mindtree.constants

object Constants {
  val JobName="WebAnalytics-Data-Ingestion"
val SingleQuote:String="\""
  val EmptyString:String=""
  val WhiteSpace=" "
  val NotApplicable="NA"
  val Period="."
  val Comma=","

  val TableJoinType="leftouter"
  val HdfsPath="HdfsPath"
  val HdfsFileName="HdfsFileName"
  val IpCountryMappingPath="IpCountryMappingPath"
  val IpCountryMapFileName="IpCountryMapFileName"
  val HivePath="HivePath"
  val IpCountryMapTable="ip_country_mapping"
  val RequestTable="request"
  val WebAnalyticsTier1Table="web_analytics_tier1"

  val IpValeColName="ipValue"
  val StartIpColName="startIp"
  val EndIpColName="endIp"
  val MaskedHostColName="masked_host"
  val HostNameColName="hostName"
  val TimestampColName="timestamp"
  val CountryNameColName="countryName"
  val CountryNameAlias="country"
  val HttpMethodColName="httpMethod"
  val HttpMethodAlias="httpmethod"
  val UriColName="uri"
  val ResponseCodeColName="responseCode"
  val ResponseCodeAlias="responsecode"
  val BytesColName="bytes"


  val HttpMethods=Array("GET","POST","PUT","DELETE","PATCH")
  val FileExtensions=Array(".wav", ".JPG",".GIF", ".gif", ".tle", ".exe", ".new", ".avi",".zip", ".txt",".jpg")
  val ALL=Array(".bad", ".cgi", ".bps", ".wav", ".JPG", ".pub", ".htw", ".GIF", ".oms", ".pdf", ".bmp", ".eps", ".mpg", ".xbm", ".hml", ".WAV", ".art", ".gif", ".tle", ".exe", ".new", ".avi", ".old", ".doc", ".COM", ".edu", ".ini", ".com", ".gov", ".sta", ".hlt", ".F.O", ".bak", ".net", ".ksc", ".vpy", ".mir", ".HTM", ".pl?", ".out", ".htm", ".zip", ".txt", ".map", ".jpg")

}
