package com.jesgoo.wharf.core.config

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.io.Source
class WharfConf(loadDefaults: Boolean = true) extends Cloneable {

  private val settings = new HashMap[String, String]()

  if (loadDefaults) {
    for ((k, v) <- System.getProperties.asScala if k.startsWith("WHARF.")) {
      settings(k) = v
    }
    if(!settings.contains("WHARF.CONFIG")){
      settings("WHARF.CONFIG")="conf"
    }
    val conf_dir = settings("WHARF.CONFIG")
    for(line <- Source.fromFile(conf_dir+"/wharf-site.conf").getLines()){
      if(!line.startsWith("#") && line.trim()!=""){
         val tmp = line.split("=", 2)
         settings(tmp(0)) = tmp(1)
      }  
    }
  }
  override def clone: WharfConf = {
    new WharfConf(false).setAll(settings)
  }
  def setAll(settings: Traversable[(String, String)]) = {
    this.settings ++= settings
    this
  }

  def set(k: String, v: String): WharfConf = {
    if (k == null) {
      throw new NullPointerException("null key")
    }
    if (v == null) {
      throw new NullPointerException("null value")
    }
    this
  }

  def get(k: String): String = {
    settings.getOrElse(k, throw new NoSuchElementException(k))
  }

  def get(key: String, defaultValue: String): String = {
    settings.getOrElse(key, defaultValue)
  }

  def getAll: Array[(String, String)] = settings.clone().toArray

  def getOption(key: String): Option[String] = {
    settings.get(key)
  }

  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }
}