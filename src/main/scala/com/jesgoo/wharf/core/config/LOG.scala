package com.jesgoo.wharf.core.config

import java.text.SimpleDateFormat

object LOG {
  var LEVEL = 1
  val logFormat = new SimpleDateFormat("[ yyyy-MM-dd HH:mm:ss_SS ]")

  def info(log: String*) {
    val str = new StringBuilder
    str.append("[INFO] ").append(logFormat.format(Utils.format_time(System.currentTimeMillis())))
    for(tmp <- log){
      str.append(" ").append(tmp)
    }
    println(str.toString())
  }

  def debug(log: String*) {
    if (LEVEL == 2) {
       val str = new StringBuilder
       str.append("[DEBUG] ").append(logFormat.format(Utils.format_time(System.currentTimeMillis())))
       for(tmp <- log){
          str.append(" ").append(tmp)
        }
    println(str.toString())
    }
  }
  
  def error(log: String*) {
    val str = new StringBuilder
    str.append("[ERROR] ").append(logFormat.format(Utils.format_time(System.currentTimeMillis())))
    for(tmp <- log){
      str.append(" ").append(tmp)
    }
    println(str.toString())
  }
  
  def setLEVEL(level:String){
    if(level == "info"){
      LEVEL=1
    }else if(level == "debug"){
      LEVEL=2
    }
  }
}