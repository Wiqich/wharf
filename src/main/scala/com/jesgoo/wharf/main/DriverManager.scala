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
import com.jesgoo.wharf.thrift.wharfconn.Response

class DriverManager() extends Runnable {

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
    var res: Response = null
    var count = 0
    while (res == null || res.port == 0) {
      try {
        res = Worker.hello_client.hello(new Request(Worker.hostname, System.currentTimeMillis(), tmpline))
      } catch {
        case e: Exception =>
          LOG.error(logger, "Connection to hello server; now reinit helloClient")
          Worker.initClient()
      }
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

      val getter = this.initGetter(tmpline, hamal)

      val d = new Driver(Worker.hostname + "_" + tmpline)
      d.init(getter, hamal, null)
      LOG.debug(logger, "Driver init " + tmpline + " success and add into DriverManager")
      addDriver(d)
      LOG.info(logger, "Worker init success=", tmpline)
    }
  }

  def recoverPusher(dr: Driver) {
    if (dr.pusher != null) {
      dr.pusher.stop()
    }
    val p = initPusher(dr.getItemName, dr.hamal)
    dr.pusher = p
    new Thread(p).start
  }

  def run() {
    isRun = true
    while (isRun) {
      for (d <- driver_map.keySet) {
        val dri = driver_map(d)
        if (dri.pusher != null && !dri.pusher.mystatus) {
          LOG.warn(logger, "Pusher", d, "status is false; now recover pusher")
          recoverPusher(dri)
          LOG.info(logger, "Pusher",dri.getItemName,"init success")
        } else if (dri.pusher == null) {
          LOG.info(logger, "Pusher", d, "first init")
          recoverPusher(dri)
        }
      }
      Thread.sleep(2000)
    }
  }
}