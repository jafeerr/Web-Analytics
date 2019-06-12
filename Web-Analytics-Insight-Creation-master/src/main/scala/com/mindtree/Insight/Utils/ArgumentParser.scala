package com.mindtree.Insight.Utils

import com.mindtree.Insight.Constants.Constants.DateFormat
import com.mindtree.Insight.jobs.Main.logger

object ArgumentParser {
  def getInsightDate(args: Array[String]) = {

    var date = DateUtil.getSystemDate()
    if (args.length > 1) {
      try {
        date = DateUtil.parseDate(args(1), DateFormat)
      }
      catch {
        case e: java.text.ParseException =>
          logger.error("Input Date is not in specified format. Insight will be created for System date.")
      }
    }
    date
  }

  def getPageFilter(args: Array[String]): Int = {
    var pageFilter: Int = 10
    if (args.length > 0) {
      try {
        pageFilter = args(0).toInt
      }
      catch {
        case e: NumberFormatException =>
          logger.error("Invalid Page filter Number Insight will be created with default of 10")
      }
    }
    pageFilter
  }
}
