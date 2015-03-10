package com.jesgoo.wharf.main

import com.jesgoo.wharf.core.config.WharfConf
import com.jesgoo.wharf.core.config.WharfContext
import com.jesgoo.wharf.merger.dealer.DealerManager
import com.jesgoo.wharf.core.config.LOG

object Merger {
    private val wc = new WharfConf(true)
    var hostname = wc.get("wharf.local.hostname", "localhost")
    val context  = new WharfContext(wc)
    
    def main(args:Array[String]){
      LOG.info("start merger......")
      LOG.info("Merger set log level to",context.LOG_LEVEL)
      LOG.setLEVEL(context.LOG_LEVEL)
      val dealerM = new DealerManager
      
      val helloServer = new ThriftHelloServer(dealerM)
      helloServer.run()
      LOG.info("start merger......end")
    }
}