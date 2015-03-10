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

class ThriftHelloServer(dealerM: DealerManager) extends WharfConnService.Iface with Runnable {

  val host_map = new HashMap[String, Host]
  val port_set = new HashSet[Int]

  var wharfConnServer : WharfConnServer = null ;
  val hello_server_port  = Merger.context.HELLO_SERVER_PORT
  
  val min_port: Int = 20000
  val max_port: Int = 50000
  val limit_get: Int = 10

  override def ping(): Boolean = true

  override def hello(req: Request): Response = {
    LOG.info("ThriftHelloServer has one client connection , host=",req.hostname," file=",req.filename)
    val host_key = req.getHostname + "_" + req.getFilename
    var port = choosePort
    if (host_map.contains(host_key)) {
      port = host_map(host_key).port
      dealerM.delDealer(host_key)
    }
    var isok = dealerM.addDealer(host_key, port)
    
    if (!isok) {
      var count = 1
      port = choosePort
      while (!isok && count < limit_get) {
        while (port_set.contains(port)) {
          port = choosePort
          Thread.sleep(1000)
        }
        isok = dealerM.addDealer(host_key, port)
        count += 1
        Thread.sleep(1000)
      }
    }
    if (!isok) {
      port = 0
      LOG.error("ThriftHelloServer mk port fail and return port = 0 ; key=",host_key)
    } else {
      val host = new Host(host_key, port, req)
      port_set += port
      host_map(host_key) = host
      LOG.info("ThriftHelloServer DealerManager add dealer success port=",String.valueOf(port)," key=",host_key)
    }
    new Response(port)
  }

  def run() {
    if(wharfConnServer == null){
      wharfConnServer = new WharfConnServer(hello_server_port)
      wharfConnServer.init(this)
    }
    LOG.info("Merger HelloServer start")
    wharfConnServer.start()
     LOG.info("Merger HelloServer start end")
  }

  def choosePort(): Int = scala.math.abs(Random.nextInt()) / max_port + min_port
}