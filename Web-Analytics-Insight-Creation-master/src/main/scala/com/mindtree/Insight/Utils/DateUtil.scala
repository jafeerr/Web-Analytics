package com.mindtree.Insight.Utils

import java.util.{Calendar, Date}

import org.apache.commons.lang.time.DateUtils

object DateUtil {
  def parseDate(input: String, format: String) = {
    val formatter = new java.text.SimpleDateFormat(format)
    formatter.parse(input)
  }

  def getSystemDate():Date={
    val calender:Calendar=Calendar.getInstance()
    calender.getTime
  }

  def getPreviousDate(date:Date) =
  {
    DateUtils.addDays(date,-1)
  }
}
