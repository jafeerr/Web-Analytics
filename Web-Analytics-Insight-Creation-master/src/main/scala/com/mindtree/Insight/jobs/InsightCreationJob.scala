package com.mindtree.Insight.jobs

import java.util.Date

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector._
import com.mindtree.Insight.Constants.Constants._
import com.mindtree.Insight.Entities.DailyInsight
import com.mindtree.Insight.Utils.DateUtil
import org.apache.commons.lang.time.DateUtils
import org.apache.spark.sql.functions.{unix_timestamp, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

class InsightCreationJob(spark: SparkSession, pageFilter: Int, inputDate: Date) extends Serializable {
  val logger = LoggerFactory.getLogger(JobName)

  import spark.sqlContext.implicits._

  def process(): Unit = {


    val sessionDF = spark.read.parquet("D:\\frameworks\\Web-Analytics-Session-Creation\\data")
    //val sessionDF = spark.read.table(SessionTable)
    val filteredSessionDF = sessionDF.filter(x => DateUtils.isSameDay(x.getTimestamp(2), inputDate))
    val previousDaySessionDF = spark.sparkContext.broadcast(sessionDF
      .filter(x => DateUtils.isSameDay(DateUtil.getPreviousDate(inputDate), x.getTimestamp(2))))
    val pageDF = spark.read.parquet("D:\\frameworks\\Web-Analytics-Data-Ingestion\\data")
    //val pageDF = spark.read.table(Tier1TableName)
    val filteredPageDF = pageDF.filter(x => DateUtils.isSameDay(DateUtil.parseDate(x.getString(1),
      TimestampFormat), inputDate))

    logger.debug("Calculating No.of Unique Visitors,Total User Sessions,Average session Time and Total Page Views")
    val result = filteredSessionDF
      .agg(countDistinct(MaskedHostColName).alias(UniqueVisitorsAlias)
        , count(MaskedHostColName).alias(TotalUserSessionsAlias),
        avg(unix_timestamp(filteredSessionDF.col(SessionEndTimeColName))
          - unix_timestamp(filteredSessionDF.col(SessionStartTimeColName))).alias(AvgSessionTimeAlias), sum(TotalRequests)
          .alias(TotalPageViewsAlias))

    val firstResult = result.first()
    val totalUserSessions = firstResult.getLong(firstResult.fieldIndex(TotalUserSessionsAlias))

    logger.debug("Finding Popular pages for the day")
    val popularPages = getPopularPages(filteredPageDF)


    logger.debug("Finding Pages with Highest Exit rates for the day")
    val pagesWithExitRates = getHigherExitRatePages(filteredSessionDF)

    logger.debug("Finding bounce rate and bounce percent")
    val bounceRate = filteredSessionDF.where(filteredSessionDF.col(TotalRequests) === 1).count()
    val bounceRatePercent: Double = (bounceRate.toDouble / (totalUserSessions)) * 100

    logger.debug("Finding Total Page Views")
    val totalHits = firstResult.getLong(firstResult.fieldIndex(TotalPageViewsAlias))

    logger.debug("Finding Traffic By Countries")
    val trafficByCountries = getTrafficByCountries(totalHits, filteredPageDF)

    logger.debug("Finding No Of New Users for the day")
    val noOfNewUsers = getNoOfNewUsers(filteredSessionDF,
      previousDaySessionDF.value)

    val insight = DailyInsight(new java.sql.Date(inputDate.getTime),
      UUIDs.timeBased(),
      firstResult.getLong(firstResult.fieldIndex(UniqueVisitorsAlias)),
      firstResult.getLong(firstResult.fieldIndex(TotalUserSessionsAlias)),
      firstResult.getDouble(firstResult.fieldIndex(AvgSessionTimeAlias)),
      totalHits,
      popularPages, pagesWithExitRates, bounceRate, bounceRatePercent, trafficByCountries, noOfNewUsers)
    print(insight)
    //saveToCassandra(insight)

  }

  def getHigherExitRatePages(filteredSessionDF: DataFrame) = {
    filteredSessionDF.groupBy(LastPageColName).agg(count(LastPageColName).alias(ExitRateAlias))
      .orderBy($"exit_rate".desc).limit(pageFilter).map(x => (x.getString(0), x.getLong(1))).collect().toMap
  }

  def getPopularPages(filteredPageDF: DataFrame) = {
    filteredPageDF.groupBy(URIColName).agg(count(URIColName).alias(PageViewsAlias))
      .orderBy($"page_views".desc).limit(pageFilter).map(x => (x.getString(0), x.getLong(1))).collect().toMap
  }

  def getNoOfNewUsers(filteredSessionDF: DataFrame, sessionDF: DataFrame) = {
    filteredSessionDF.where(!filteredSessionDF.col(MaskedHostColName).
      isin(sessionDF.filter(x => (!DateUtils.isSameDay(DateUtil.parseDate(x.getString(1), TimestampFormat), inputDate)))
        .col(MaskedHostColName))).count()
  }

  def saveToCassandra(insight: DailyInsight): Unit = {
    val sparkContext = spark.sparkContext
    val dailyInsightRdd = sparkContext.parallelize(List(insight))
    dailyInsightRdd.saveToCassandra(KeySpaceName, InsightTable)
  }

  def getTrafficByCountries(totalHits: Long, dataFrame: DataFrame): Map[String, Double] = {

    dataFrame.groupBy(CountryCodeColName).agg(((count(URIColName) / totalHits) * 100).alias(CountryTrafficAlias))
      .orderBy(CountryCodeColName).map(x => convertToMap(x)).collect().toMap
  }

  def convertToMap(row: Row) = {
    var country = row.getString(0)
    if (country == null || country.isEmpty)
      country = CountryOthers
    (country, row.getDouble(1))
  }

  def getValue(row: Row, columnName: String) = {
    row.get(row.fieldIndex(columnName))
  }

}
