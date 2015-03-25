package com.jesgoo.wharf.worker.pusher

import org.apache.log4j.Logger

import com.jesgoo.wharf.core.client.WharfDataClient
import com.jesgoo.wharf.core.config.LOG
import com.jesgoo.wharf.main.Worker
import com.jesgoo.wharf.thrift.wharfdata.Event
import com.jesgoo.wharf.thrift.wharfdata.EventType
import com.jesgoo.wharf.thrift.wharfdata.Head
import com.jesgoo.wharf.worker.hamal.Hamal

class ThriftPusher(port: Int) extends Pusher {

  var hamal: Hamal = null

  var client: WharfDataClient = null
  var isRun = false

  val retry_limit = 0

  val fail_count_limit = 3

  val pusher_period = Worker.context.THRIFT_PUSHER_PERIOD
  val pusher_host = Worker.context.THRIFT_PUSHER_HOST
  val pusher_timeout = Worker.context.THRIFT_PUSHER_TIMEOUT
  val logger = Logger.getLogger(getClass.getName)
  def init() {
    client = new WharfDataClient(pusher_host, port, pusher_timeout)
    Thread.sleep(1000)
  }

  def setHamal(hamal: Hamal) {
    this.hamal = hamal
  }

  def push(evt: Event): Boolean = {
    if (client == null) {
      this.init()
    }
    LOG.debug(logger, "Thrift pusher push event to collector")

    if (client.push(evt)) {
      LOG.debug(logger, "Thrift pusher push event to collector; result = success")
      this.hamal.sendFinish(evt.id)
      new Thread(new Runnable {
        def run() {
          LOG.debug(logger, "Thrift pusher push event to collector success ; report server to delete")
          val v = client.push(getDeltedEvent(evt.getId))
          if (!v) {
            LOG.error(logger, "Thrift pusher push deleteevent to collector fail ; id = ", evt.getId)
          }
        }
      }).start()
      true
    } else {
      LOG.error(logger, "Thrift pusher push event to collector; result = fail")
      false
    }
  }

  def getDeltedEvent(get_id: String): Event = new Event(get_id, new Head(Worker.hostname, EventType.SIGN2))
  def stop() {
    if (client != null) {
      client.close()
      client = null
    }
    isRun = false
  }

  def run() {
    mystatus = true
    isRun = true
    var fail_count = 0
    var evt = hamal.out()
    while (isRun) {
      try {
        if (evt != null) {
          val is = push(evt)
          if (is == false) {
            LOG.warn(logger, "push event server return false")
          } else {
            LOG.debug(logger, "Thrift pusher push event to collector success set event = null")
            evt = null
          }
        }
        if (evt == null) {
          evt = hamal.out()
        }
      } catch {
        case e: Exception =>
          LOG.error(logger, "Thrift pusher post data error ; failcount=",fail_count) 
          fail_count += 1
          if (fail_count > fail_count_limit) {
            mystatus = false
            isRun = false
            LOG.error(logger, "Thrift pusher post data error ; failcount > limits=",fail_count_limit,"now stop this thrift to restart") 
            if (client != null) {
                client.close
            }
          }
      }
      Thread.sleep(pusher_period)
    }
  }
}