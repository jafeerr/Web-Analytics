package com.mindtree.util

import java.io.File

import com.mindtree.constants.Constants._
import com.mindtree.entities.PathDetails
import com.typesafe.config.ConfigFactory

object ConfigUtils {
  def getPathDetails(configFilePath: String) = {

    val config = ConfigFactory.parseFile(new File(configFilePath))

    PathDetails(config.getString(HdfsPath), config.getString(HdfsFileName),
      config.getString(IpCountryMappingPath), config.getString(IpCountryMapFileName),config.getString(HivePath))
  //PathDetails("hdfs://ip-10-0-1-20.ec2.internal:8020/user/bdhlabuser113/web_request_logs/","access_log_Jul95","hdfs://ip-10-0-1-20.ec2.internal:8020/user/bdhlabuser113/IpCountryMapping/","GeoIPCountryWhois.csv","hdfs://ip-10-0-1-20.ec2.internal:8020/user/bdhlabuser113/IpCountryMapping/Hive_tier1_storage")
  }
}
