package com.jesgoo.wharf.merger.dealer

import java.util.List
import com.jesgoo.wharf.core.config.Utils
import com.jesgoo.wharf.core.server.WharfDataServer
import com.jesgoo.wharf.merger.puller.Puller
import com.jesgoo.wharf.thrift.wharfdata.Content
import com.jesgoo.wharf.thrift.wharfdata.Event
import com.jesgoo.wharf.thrift.wharfdata.WharfDataService
import com.jesgoo.wharf.thrift.wharfdata.EventType
import com.jesgoo.wharf.core.config.LOG
import org.apache.log4j.Logger

class ThriftEventDealer(port: Int) extends Dealer with WharfDataService.Iface {

  var wharfDataServer: WharfDataServer = null;
  var puller: Puller = null

  var puller_thread: Thread = null

  val eventIdsM = new EventIdsManager
  val logger = Logger.getLogger(getClass.getName)
  def init() {
    if (wharfDataServer != null) {
      wharfDataServer.close()
    }
    wharfDataServer = new WharfDataServer(port)
    wharfDataServer.init(this)
  }

  def stop() {
    if (wharfDataServer != null) {
      wharfDataServer.close()
    }
    if (puller != null) {
      puller.stop()
    }
  }

  def run() {
    init()
    new Thread(eventIdsM).start()
    try{
        wharfDataServer.start()
    }catch{
      case e:Exception =>
        e.printStackTrace()
    }
  }

  def setPuller(p: Puller) {
    if (puller == null) {
      puller = p
    }
  }

  def checkMd5(event: Event): Boolean = {
    val data: List[Content] = event.body.getContents()
    for (i <- 0 to data.size() - 1) {
      val d = data.get(i)
      val tmp_md5 = Utils.md5(d.rel)
      if (tmp_md5 != d.md5) {
        LOG.error(logger,"ThriftEventDealer check md5 fail; src_md5=", d.md5," dest_md5=",tmp_md5," data=",d.rel)
        false
      }
    }
    true
  }

  override def push(event: Event): Boolean = {
    if (event == null) {
      false
    }
    LOG.debug(logger,"ThriftEventDealer get a event is=", event.getId,"eventIds cache size=",String.valueOf(eventIdsM.length()))
    if (event.getHead.getType == EventType.SIGN2) {
      eventIdsM.remove(event.getId)
      true
    } else {
      if (!checkMd5(event)) {
        false
      }
      if (!puller.pull(event)) {
        false
      }
      eventIdsM.putdata(event.getId, System.currentTimeMillis())
      true
    }
  }

  override def ping(): Boolean = true
}