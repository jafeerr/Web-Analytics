package com.mindtree.jobs

import com.mindtree.util.ConfigUtils
import com.mindtree.constants.Constants.JobName
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


object Main extends App {
  val logger = LoggerFactory.getLogger(JobName)
  val spark = SparkSession.builder().appName(JobName).master("local")
    //.enableHiveSupport()
    .config("hive.exec.dynamic.partition","true")
    .config("hive.exec.dynamic.partition.mode","nonstrict").getOrCreate()
  if (args.length > 0) {
    val configPath = args(0)
    val pathDetails = ConfigUtils.getPathDetails(configPath)
    val ingestionJob = new DataIngestionJob(spark,pathDetails.hdfsPath + pathDetails.hdfsFileName,
      pathDetails.ipCountryMapFilePath + pathDetails.ipCountryMapFileName,pathDetails.hivePath)
    try {
      ingestionJob.process()
    }
    catch{
      case e:org.apache.hadoop.mapred.InvalidInputException=>
        logger.error("Input File/Path does not exists")
    }
  }
  else {
    logger.error("Connection configuration not given")
  }
}
