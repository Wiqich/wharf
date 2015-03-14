package com.jesgoo.wharf.main

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.Random
import com.jesgoo.wharf.core.Host
import com.jesgoo.wharf.merger.dealer.DealerManager
import com.jesgoo.wharf.thrift.wharfconn.Request
import com.jesgoo.wharf.thrift.wharfconn.Response
import com.jesgoo.wharf.thrift.wharfconn.WharfConnService
import com.jesgoo.wharf.core.server.WharfConnServer
import com.jesgoo.wharf.core.config.LOG
import org.apache.log4j.Logger

class ThriftHelloServer(dealerM: DealerManager) extends WharfConnService.Iface with Runnable {

  val host_map = new HashMap[String, Host]
  val port_set = new HashSet[Int]

  var wharfConnServer: WharfConnServer = null;
  val hello_server_port = Merger.context.HELLO_SERVER_PORT

  val min_port: Int = 20000
  val max_port: Int = 50000
  val port_cyle_limit = 10

  val logger = Logger.getLogger(getClass.getName)
  override def ping(): Boolean = true

  override def hello(req: Request): Response = {
    LOG.info(logger, "ThriftHelloServer has one client connection , host=", req.hostname, " file=", req.filename)
    val host_key = req.getHostname + "_" + req.getFilename
    var port = choosePort
    if (host_map.contains(host_key)) {
      port = host_map(host_key).port
    } else {
      var count = 0
      while(port_set.contains(port) && count < port_cyle_limit){
        port = choosePort
        count +=1
        LOG.info(logger, "choose port this count=",count)
        Thread.sleep(1000)
      }
      var isok = dealerM.addDealer(host_key, port)
      if (!isok) {
        port = 0
        LOG.error(logger, "ThriftHelloServer mk port fail and return port = 0 ; key=", host_key)
      } else {
        val host = new Host(host_key, port, req)
        port_set += port
        host_map(host_key) = host
        LOG.info(logger, "ThriftHelloServer DealerManager add dealer success port=", String.valueOf(port), " key=", host_key)
      }
    }
    new Response(port)
  }

  def run() {
    if (wharfConnServer == null) {
      wharfConnServer = new WharfConnServer(hello_server_port)
      wharfConnServer.init(this)
    }
    LOG.info(logger, "Merger HelloServer start")
    wharfConnServer.start()
  }

  def choosePort(): Int = scala.math.abs(Random.nextInt()) / max_port + min_port
}