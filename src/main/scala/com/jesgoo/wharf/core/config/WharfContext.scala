package com.jesgoo.wharf.core.config

class WharfContext(config:WharfConf) {
  
  //def getWharfConf():WharfConf = config
  
  val GET_LOG_LIST = config.get("worker.getlog.list","")
  
  val DRIVER_PERIOD = config.getInt("worker.driver.period",1000)
  
  val HELLO_SERVER_PORT = config.getInt("merger.helloserver.port", 9008)
  
  val EVENT_ID_MANAGER_RETAIN : Long = config.getLong("merger.eidmng.retain", 600000)
  val EVENT_ID_MANAGER_CLEAR_PERIOD : Long = config.getLong("merger.eidmng.clear.period", 600000)
  val EVENT_ID_MANAGER_CLEAR_SIZE : Long = config.getLong("merger.eidmng.clear.size", 100)
  
  val LOG_LEVEL :String = config.get("log.level", "info")
  
  //filepuller
  val FILE_PULLER_FILEPATH = config.get("merger.filepuller.filepath", "/tmp/logs")
  val FILE_PULLER_TOLL_TYPE = config.get("merger.filepuller.roll.type", "hourly")
  val FILE_PULLER_PERIOD = config.getLong("merger.filepuller.period", 10000)
  
  //store hamal
  val STORE_HAMAL_IN_LINES = config.getInt("worker.storehamal.inlines", 20)
  val STORE_HAMAL_IN_TIMEOUT = config.getInt("worker.storehamal.intimeout", 10000)
  val STORE_HAMAL_PERIOD = config.getInt("worker.storehamal.period", 2000)
  val STORE_HAMAL_PATH = config.get("worker.storehamal.path", "/tmp/hamal")
  
  //thrift pusher
  
  val THRIFT_PUSHER_PERIOD = config.getInt("worker.thriftpusher.period", 1000)
  val THRIFT_PUSHER_HOST = config.get("worker.thrift.pusher.host", "127.0.0.1")
  val THRIFT_PUSHER_TIMEOUT = config.getInt("worker.thriftpusher.thrift.timeout", 60000)
  
  //exit code
  val WORK_LOG_NULL = 1
  val WORK_PING_MERGER_ERROR = 2
  val WORK_CONNECTION_REFUSED = 3
  val WORK_INIT_DRIVERS_ERROR = 4
  
}