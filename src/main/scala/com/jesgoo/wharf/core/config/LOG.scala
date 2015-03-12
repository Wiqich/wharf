package com.jesgoo.wharf.core.config

import java.text.SimpleDateFormat
import org.apache.log4j.Logger

object LOG {

  def info(logger :Logger , log: Any*) {
    val str = new StringBuilder
    for (tmp <- log) {
      str.append(tmp).append(" ")
    }
    logger.info(str.toString().trim())
  }

  def debug(logger :Logger ,log: Any*) {
    val str = new StringBuilder
    for (tmp <- log) {
      str.append(tmp).append(" ")
    }
    logger.debug(str.toString().trim())
  }
  def error(logger:Logger,log: Any*) {
    val str = new StringBuilder
    for (tmp <- log) {
      str.append(tmp).append(" ")
    }
    logger.error(str.toString().trim())
  }

  def fatal(logger:Logger,log: Any*) {
    val str = new StringBuilder
    for (tmp <- log) {
      str.append(tmp).append(" ")
    }
    logger.fatal(str.toString().trim())
  }
}