package com.jesgoo.wharf.main

import com.jesgoo.wharf.core.config.WharfConf
import com.jesgoo.wharf.core.config.WharfContext
import com.jesgoo.wharf.merger.dealer.DealerManager
import com.jesgoo.wharf.core.config.LOG
import org.apache.log4j.Logger

object Merger {
    private val wc = new WharfConf(true)
    var hostname = wc.get("wharf.local.hostname", "localhost")
    val context  = new WharfContext(wc)
    val logger = Logger.getLogger(getClass.getName)
    def main(args:Array[String]){
      LOG.info(logger,"start merger......")
      LOG.debug(logger,"Merger set log level to")
      val dealerM = new DealerManager
      
      val helloServer = new ThriftHelloServer(dealerM)
      helloServer.run()
    }
}