package com.mindtree.util

import com.mindtree.constants.Constants._
import com.mindtree.entities.{IPCountryMapping, Request}
import com.mindtree.jobs.Main.logger
import org.apache.ivy.util
import com.mindtree.exceptions.InvalidHttpUri
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object InputParser {
  val logger = LoggerFactory.getLogger(JobName)
  val HostNameRegex ="""[a-zA-Z0-9.\-_]{1,}""".r
  val TimeStampRegex ="""\d{1,2}\/\w{1,3}\/\d{4}:\d{2}:\d{2}:\d{2} -\d{4}""".r
  val RequestRegex = "\".*\"".r

  def parseLine(input: String): Option[Request] = {
    try {
      val httpRequest = getHttpRequest(input)
      var httpMethod = EmptyString
      var uri = EmptyString
      if (HttpMethods.contains(httpRequest(0))) {
        httpMethod = httpRequest(0)
        uri = httpRequest(1)
      }
      else {
        uri = httpRequest(0)
      }
      val inputs = input.split(WhiteSpace)
      val ipAddress = getOrEmptyString(inputs, 0)
      Some(Request(ipAddress, TimeStampRegex.findFirstIn(input).get.split(WhiteSpace)(0), Common.ipToLong(ipAddress), httpMethod, uri, getInt(inputs.length - 2, inputs), getInt(inputs.length - 1, inputs)))
    }
    catch {
      case (e:InvalidHttpUri)=>
        logger.error("Error While Parsing Raw Input "+ e)
        None
      case (e: Exception) =>
        logger.error("Error While Parsing Raw Input " + util.StringUtils.getStackTrace(e))
        None

    }
  }
private def getHttpRequest(input:String) =
  {
      val httpRequest = RequestRegex.findFirstIn(input)
        if(httpRequest!=None)
          httpRequest.get.replace(SingleQuote, EmptyString).split(WhiteSpace)
      else
         throw new InvalidHttpUri("URI is Invalid or not Present")

  }
  def getOrEmptyString(inputs: Array[String], index: Int) = {
    if (index <= inputs.length - 1)
      inputs(index)
    else
      EmptyString
  }

  private def getInt(index: Int, inputs: Array[String]): Int = {
    Try {
      inputs(index).toInt
    } match {
      case Success(value) => value
      case Failure(ex) =>
        0
    }
  }

  def parseIpCountryMap(input: String) = {
    val inputs = input.replace(SingleQuote, EmptyString).split(Comma)
    IPCountryMapping(Common.ipToLong(inputs(0)), Common.ipToLong(inputs(1)), inputs(4),inputs(5))
  }

}
