package com.jesgoo.wharf.main

import com.jesgoo.wharf.core.client.WharfConnClient
import com.jesgoo.wharf.core.config.WharfConf
import com.jesgoo.wharf.core.config.WharfContext
import com.jesgoo.wharf.core.config.LOG

object Worker {
  
    val start_time = System.currentTimeMillis()
    private val wc = new WharfConf(true)
    val hostname = wc.get("worker.thrift.pusher.host", "localhost")
    val context  = new WharfContext(wc)
    
    val hello_port = context.HELLO_SERVER_PORT
    var hello_client :WharfConnClient= null 
    
    val driverManager = new DriverManager()
    
    val IsRUN = true
    def main(args:Array[String]){
      LOG.info("start worker ",hostname)
      val log_list = context.GET_LOG_LIST
      LOG.info("Woker set log level to",context.LOG_LEVEL)
      LOG.setLEVEL(context.LOG_LEVEL)
      if(log_list == ""){
        LOG.error("list log is null ; error exit")
        System.exit(context.WORK_LOG_NULL)
      }
      if(hello_client == null){
        try{
          hello_client = new WharfConnClient(hostname,hello_port)
        }catch{
          case e: Exception  =>
          e.printStackTrace()
          LOG.error(" Worker init helloClient error and exit",String.valueOf(context.WORK_CONNECTION_REFUSED))
          System.exit(context.WORK_CONNECTION_REFUSED)
        }
      }
      if(!hello_client.ping()){
        System.exit(context.WORK_PING_MERGER_ERROR)
      }
      try{
        driverManager.initByLogList(log_list)
      }catch{
        case e: Exception  =>
        e.printStackTrace()
        System.exit(context.WORK_INIT_DRIVERS_ERROR)
      }
      while (true){
           if(hello_client != null ){
             hello_client.ping()
           }
           Thread.sleep(2000)
      }
    }
}
