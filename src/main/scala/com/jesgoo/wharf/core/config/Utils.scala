package com.jesgoo.wharf.core.config

import java.text.SimpleDateFormat
import java.util.Date

object Utils {
  def md5(value: String): String = {

    lazy val md5handle = java.security.MessageDigest.getInstance("MD5")
    val hexDigits = Array[Char]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

    val encrypt = md5handle.digest(value.getBytes)
    val b = new StringBuilder(32)
    for (i <- 0.to(15)) {
      b.append(hexDigits(encrypt(i) >>> 4 & 0xf)).append(hexDigits(encrypt(i) & 0xf))
    }
    b.mkString
  }
  
  def mk_file_tail_name(minuteFormat:SimpleDateFormat,ts:Long):String={
    minuteFormat.format(format_time(ts))+"0000"
  }
  
  def format_time(ts:Long):Long= {
    if(String.valueOf(ts).length() == 10){
      ts * 1000
    }
    ts
  }
}
