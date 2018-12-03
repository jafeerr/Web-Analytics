package com.mindtree.jobs

import com.mindtree.constants.Constants._
import com.mindtree.entities.Request
import com.mindtree.util.InputParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

class DataIngestionJob(spark: SparkSession, hdfsPath: String, ipCountryMapPath: String, hivePath: String) extends Serializable {

  val logger = LoggerFactory.getLogger(JobName)
  def process(): Unit = {

    val sparkContext = spark.sparkContext
    val sqlContext = spark.sqlContext
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    import sqlContext.implicits._
    //load ip country mapping table
    val ipCountryMapDF = sparkContext.textFile(ipCountryMapPath).map(x => InputParser.parseIpCountryMap(x)).toDF()
    val broadCastedMap=sparkContext.broadcast(ipCountryMapDF)
    logger.info("Ip Country Mapping file loaded into data frame")

    //load HDFS data
    val data = sparkContext.textFile(hdfsPath)
    val validRecords = getValidRecords(data)
    val inputDF = validRecords.toDF()
    logger.info("Input(HDFS data) loaded into data frame")


    val joinedDF = inputDF.join(broadCastedMap.value, inputDF.col(IpValeColName)
      .between(broadCastedMap.value.col(StartIpColName) , broadCastedMap.value.col(EndIpColName)),TableJoinType)
      .withColumn(MaskedHostColName, sha1(inputDF.col(HostNameColName)))

      val result=joinedDF.select(joinedDF.col(MaskedHostColName), joinedDF.col(TimestampColName),
      joinedDF.col(CountryNameColName).alias(CountryNameAlias), joinedDF.col(HttpMethodColName).alias(HttpMethodAlias),
      joinedDF.col(UriColName), joinedDF.col(ResponseCodeColName).alias(ResponseCodeAlias), joinedDF.col(BytesColName))
    
   //result.write.insertInto(WebAnalyticsTier1Table) //cluster
    result.write.mode(SaveMode.Overwrite).format("parquet").save("data")//local
  }


  private def getValidRecords(data: RDD[String]) = {
    data.map(x => InputParser.parseLine(x)).filter(x => isValid(x)).repartition(3).map(x => x.get)
  }

  private def isValid(input: Option[Request]): Boolean = {
    input != None && (!input.get.uri.isEmpty)&&
      (!input.get.uri.contains(".gif"))&&
      (!input.get.uri.contains(".GIF"))//&& Common.isNotFileRequest(input.get.uri)
  }

}
