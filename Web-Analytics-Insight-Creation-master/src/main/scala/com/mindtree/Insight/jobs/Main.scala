package com.mindtree.Insight.jobs

import com.mindtree.Insight.Constants.Constants._
import com.mindtree.Insight.Utils.ArgumentParser
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Main extends App {
  val logger = LoggerFactory.getLogger(JobName)

  val spark = SparkSession.builder().appName(JobName).master("local")
    //.enableHiveSupport()
    .config("hive.exec.dynamic.partition", "true")
    .config("spark.cassandra.connection.host", "10.0.1.10")
    .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()

  val insightCreationJob = new InsightCreationJob(spark, ArgumentParser.getPageFilter(args),
    ArgumentParser.getInsightDate(args))
  insightCreationJob.process()
}
