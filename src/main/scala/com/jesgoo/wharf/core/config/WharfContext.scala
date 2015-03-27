package com.jesgoo.wharf.core.config

class WharfContext(config:WharfConf) {
  
  //def getWharfConf():WharfConf = config
  
  val GET_LOG_LIST = config.get("worker.getlog.list","")
  
  val DRIVER_PERIOD = config.getInt("worker.driver.period",1000)
  
  val HELLO_SERVER_PORT = config.getInt("merger.helloserver.port", 9008)
  
  val EVENT_ID_MANAGER_RETAIN : Long = config.getLong("merger.eidmng.retain", 600000)
  val EVENT_ID_MANAGER_CLEAR_PERIOD : Long = config.getLong("merger.eidmng.clear.period", 600000)
  val EVENT_ID_MANAGER_CLEAR_SIZE : Long = config.getLong("merger.eidmng.clear.size", 100)
  
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
  
  //kafka puller
  val PULLER_KAFKA_COMPRESS = config.getBoolean("puller.kafka.compress", false)
  val PULLER_KAFKA_BROKER_LIST = config.get("puller.kafka.broker.list", "127.0.0.1:9092")
  val PULLER_KAFKA_TOPIC = config.get("puller.kafka.topic", "default")
  val PULLER_KAFKA_BATCHSIZE = config.getInt("puller.kafka.batch.size",200)
  val PULLER_KAFKA_SYNC = config.getBoolean("puller.kafka.sync", true)
  val PULLER_KAFKA_MAXRETRY = config.getInt("puller.kafka.max.retry", 5)
  val PULLER_KAFKA_REQUEST_ACKS = config.getInt("puller.kafka.request.acks", -1)
  
  val PULLER_KAFKA_PERIOD = config.getLong("puller.kafka.run.period", 2000)
  
  val PULLER_KAFKA_STATUS_DIR = config.get("puller.kafka.status.dir","/tmp/kafka_puller/status")
  
  //
  val MERGER_PULLER_CLASS =  config.get("merger.puller.class","com.jesgoo.wharf.merger.puller.FilePuller")
  
  
  //exit code
  val WORK_LOG_NULL = 1
  val WORK_PING_MERGER_ERROR = 2
  val WORK_CONNECTION_REFUSED = 3
  val WORK_INIT_DRIVERS_ERROR = 4
  
}