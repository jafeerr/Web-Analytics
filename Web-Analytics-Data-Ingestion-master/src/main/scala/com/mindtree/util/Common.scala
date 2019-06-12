package com.mindtree.util
import com.mindtree.constants.Constants._
object Common {
  def ipToLong(input: String): Long = {
    try {
      val inputs = input.split("\\.")
      var result: Long = 0
      for (i <- 0 to inputs.length - 1) {
        result += (inputs(i).toLong) * Math.pow(256, 3 - i).toLong
      }
      result
    }
    catch {
      case ex: Exception =>
        0
    }
  }
  def isNotFileRequest(uri:String) =
  {
    for(extension<-FileExtensions)
      {
        if(uri.endsWith(extension))
          false
      }
    true
  }
}