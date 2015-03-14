package com.jesgoo.wharf.main

import scala.collection.mutable.HashMap

import org.apache.log4j.Logger

import com.jesgoo.wharf.core.client.WharfConnClient
import com.jesgoo.wharf.core.config.LOG
import com.jesgoo.wharf.thrift.wharfconn.Request
import com.jesgoo.wharf.worker.getter.Getter
import com.jesgoo.wharf.worker.getter.TailFileGetter
import com.jesgoo.wharf.worker.hamal.Hamal
import com.jesgoo.wharf.worker.hamal.StoreHamal
import com.jesgoo.wharf.worker.pusher.Pusher
import com.jesgoo.wharf.worker.pusher.ThriftPusher

class AloneDriverManager(Role:String) extends Runnable{

  val driver_map = new HashMap[String, Driver]
  val logger = Logger.getLogger(getClass.getName)
  
  var isRun = false
  def removeBykey(key: String): Boolean = {
    if (driver_map.contains(key)) {
      driver_map(key).delAll()
      driver_map -= key
      true
    }
    false
  }

  def addDriver(d: Driver) {
    driver_map(d.dName) = d
    d.todo()
  }

  def initPusher(tmpline: String, hamal: Hamal): Pusher = {
    var res = Worker.hello_client.hello(new Request(Worker.hostname, System.currentTimeMillis(), tmpline))
    var count = 0
    while (res == null || res.port == 0) {
      res = Worker.hello_client.hello(new Request(Worker.hostname, System.currentTimeMillis(), tmpline))
      count += 1
      LOG.debug(logger, "Reget res port count=", count)
      Thread.sleep(1000)
    }
    LOG.debug(logger, "Getter init with file ", tmpline)
    val tp = new ThriftPusher(res.port)
    tp.setHamal(hamal)
    tp
  }

  def initGetter(tmpline: String, hamal: Hamal): Getter = {
    LOG.debug(logger, "Getter init with file ", tmpline)
    val g = new TailFileGetter(tmpline)
    g.setHamal(hamal)
    g
  }

  def initHamal(tmpline: String): Hamal = {
    LOG.debug(logger, "Hamal init with filename ", tmpline.substring(tmpline.lastIndexOf("/") + 1))
    new StoreHamal(tmpline.substring(tmpline.lastIndexOf("/") + 1))
  }

  def initByLogList(log_list: String) {
    LOG.info(logger, "Drivaler Manager init log get ", log_list)
    for (line <- log_list.split(",")) {
      val tmpline = line.trim()

      LOG.debug(logger, tmpline)

      val hamal = this.initHamal(tmpline)
      var pusher : Pusher = null
      
      pusher = initPusher(tmpline, hamal)
      
      val getter = this.initGetter(tmpline, hamal)

      val d = new Driver(Worker.hostname + "_" + tmpline)
      d.init(getter, hamal, pusher)
      LOG.debug(logger, "Driver init " + tmpline + " success and add into DriverManager")
      addDriver(d)
      LOG.info(logger, "Worker init success=", tmpline)
    }
  }
  
  def recoverPusher(dr : Driver){
    dr.pusher.stop()
    val p = initPusher(dr.getItemName,dr.hamal)
    dr.pusher = p
    new Thread(p).start
  }
  
  def run(){
    isRun = true
    while(isRun){
      for(d <- driver_map.keySet){
        if(driver_map(d).pusher != null && !driver_map(d).pusher.mystatus){
          recoverPusher(driver_map(d))
        }
      }
      Thread.sleep(2000)
    }
  }
}