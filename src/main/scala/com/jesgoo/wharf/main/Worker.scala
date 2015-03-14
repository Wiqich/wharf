package com.jesgoo.wharf.main

import com.jesgoo.wharf.core.client.WharfConnClient
import com.jesgoo.wharf.core.client.WharfConnClient
import com.jesgoo.wharf.core.config.LOG
import com.jesgoo.wharf.core.config.WharfConf
import com.jesgoo.wharf.core.config.WharfContext
import org.apache.log4j.Logger

object Worker {
  val start_time = System.currentTimeMillis()
  private val wc = new WharfConf(true)
  val hostname = wc.get("wharf.local.hostname", "localhost")
  val context = new WharfContext(wc)

  val hello_port = context.HELLO_SERVER_PORT
  var hello_client: WharfConnClient = null

  val logger = Logger.getLogger(getClass.getName)

  val driverManager = new DriverManager()

  val IsRUN = true
  def main(args: Array[String]) {
    LOG.info(logger, "start worker ", hostname)
    val log_list = context.GET_LOG_LIST
    if (log_list == "") {
      LOG.error(logger, "list log is null ; error exit")
      System.exit(context.WORK_LOG_NULL)
    }
    try {
      driverManager.initByLogList(log_list)
      new Thread(driverManager).start
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.exit(context.WORK_INIT_DRIVERS_ERROR)
    }
  }
  def initClient() {
    if (hello_client != null) {
      hello_client.close()
    }
      var isOk = false
      while (!isOk) {
        try {
          hello_client = new WharfConnClient(context.THRIFT_PUSHER_HOST, hello_port)
          isOk = true
        } catch {
          case e: Exception =>
            LOG.error(logger, "Worker init helloClient error ; after 5s and reinit until success")
        }
        Thread.sleep(5000)
      }
  }
}
