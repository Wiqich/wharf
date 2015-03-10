package com.jesgoo.wharf.main

import scala.collection.mutable.HashMap
import com.jesgoo.wharf.worker.getter.TailFileGetter
import com.jesgoo.wharf.worker.hamal.StoreHamal
import com.jesgoo.wharf.worker.pusher.ThriftPusher
import com.jesgoo.wharf.core.client.WharfConnClient
import com.jesgoo.wharf.thrift.wharfconn.Request
import com.jesgoo.wharf.core.config.LOG

class DriverManager {

  val driver_map = new HashMap[String, Driver]
  def removeBykey(key: String): Boolean = {
    if (driver_map.contains(key)) {
      driver_map(key).delete()
      driver_map -= key
      true
    }
    false
  }

  def addDriver(d: Driver) {
    driver_map(d.dName) = d
    d.todo()
  }

  def initByLogList(log_list: String) {
    LOG.info("Drivaler Manager init log get ", log_list)
    for (line <- log_list.split(",")) {
      val tmpline = line.trim()

      LOG.debug(tmpline)

      val res = Worker.hello_client.hello(new Request(Worker.hostname, System.currentTimeMillis(), tmpline))
      LOG.debug("BOBO get port=",String.valueOf(res.port))
      if (res == null) {
        LOG.error("Response is null")
        throw new NullPointerException("Response is null")
      }
      if (res.port == 0) {
        LOG.error("Worker get port error and port = 0")
        throw new Exception("Worker get port error and port = 0")
      } else {
        LOG.debug("Hamal init with filename ", line.substring(line.lastIndexOf("/") + 1))
        val h = new StoreHamal(line.substring(line.lastIndexOf("/") + 1))
        LOG.debug("Getter init with file ", tmpline)
        val g = new TailFileGetter(tmpline)
        g.setHamal(h)
        LOG.debug("Pusher init with port ", String.valueOf(res.port))
        val p = new ThriftPusher(res.port)
        p.setHamal(h)
        val d = new Driver(Worker.hostname + "_" + tmpline)
        d.init(g, h, p)
        LOG.debug("Driver init "+tmpline+" success and add into DriverManager")
        addDriver(d)
      }
    }
  }
}