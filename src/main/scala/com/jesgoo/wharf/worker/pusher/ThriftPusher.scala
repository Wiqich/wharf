package com.jesgoo.wharf.worker.pusher

import com.jesgoo.wharf.worker.hamal.Hamal
import com.jesgoo.wharf.core.client.WharfDataClient
import com.jesgoo.wharf.thrift.wharfdata.Event
import com.jesgoo.wharf.main.Worker
import com.jesgoo.wharf.thrift.wharfdata.EventType
import com.jesgoo.wharf.thrift.wharfdata.Head
import com.jesgoo.wharf.thrift.wharfdata.Body
import com.jesgoo.wharf.core.config.LOG

class ThriftPusher(port: Int) extends Pusher {

  var hamal: Hamal = null

  var client: WharfDataClient = null

  val pusher_period = Worker.context.THRIFT_PUSHER_PERIOD
  val pusher_host = Worker.context.THRIFT_PUSHER_HOST
  val pusher_timeout = Worker.context.THRIFT_PUSHER_TIMEOUT

  def init() {
    client = new WharfDataClient(pusher_host, port, pusher_timeout)
    if (!client.ping()) {
      LOG.error("Thrift pusher helloclient ping false and reinit") 
      client = null
      client = new WharfDataClient(pusher_host, port, pusher_timeout)
    }
  }

  def setHamal(hamal: Hamal) {
    this.hamal = hamal
  }

  def push(evt: Event): Boolean = {
    if (client == null) {
      this.init()
    }
    LOG.debug("Thrift pusher push event to collector")
    var result = false
    try{
        result = client.push(evt)
    }catch{
      case e:Exception =>
        e.printStackTrace()
        LOG.debug("ThriftPusher put event fail")
        false
    }
    if (result) {
      LOG.debug("Thrift pusher push event to collector; result = success") 
      this.hamal.sendFinish(evt.id)
      new Thread(new Runnable{
        def run(){
           LOG.debug("Thrift pusher push event to collector success ; report server to delete") 
           val v = client.push(getDeltedEvent(evt.getId))
           if(!v){
              LOG.error("Thrift pusher push deleteevent to collector fail ; id = ",evt.getId) 
           }
        }
      }).start()
      true
    } else {
      LOG.error("Thrift pusher push event to collector; result = fail") 
      false
    }
  }

  def getDeltedEvent(get_id: String): Event = new Event(get_id, new Head(Worker.hostname, EventType.SIGN2))
  def stop() {
    if (client != null) {
      client = null
    }
  }

  def run() {
    var evt = hamal.out()
    while (true) {
      if (evt != null) {
        val is = push(evt)
        if (is == false) {
          if (!client.ping()) {
            client = null
          }
        } else {
          LOG.debug("Thrift pusher push event to collector success set event = null") 
          evt = null
        }
      }
      if (evt == null) {
        evt = hamal.out()
      }
      Thread.sleep(pusher_period)
    }
  }
}