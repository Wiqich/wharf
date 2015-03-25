package com.jesgoo.wharf.core

import com.jesgoo.wharf.core.server.WharfConnServer
import com.jesgoo.wharf.thrift.wharfconn.WharfConnService
import com.jesgoo.wharf.thrift.wharfconn.WharfConnService.Iface
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.text.SimpleDateFormat
import java.util.Date
import com.jesgoo.wharf.core.config.Utils
import kafka.message.DefaultCompressionCodec
import kafka.producer.KeyedMessage
import kafka.message.NoCompressionCodec
import scala.util.Properties
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import java.util.Properties
import java.util.UUID

object Main {
  
  val topic: String = "logh" 
  val brokerList: String ="192.168.2.5:9092"
  val synchronously: Boolean = true
  val compress: Boolean = false

  val batchSize: Integer = 200
  val messageSendMaxRetries: Integer = 3
  val requestRequiredAcks: Integer = 1
  
  val props = new Properties()

  val codec = if(compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec
  val clientId: String = UUID.randomUUID().toString

  props.put("compression.codec", codec.toString)
  props.put("producer.type", if(synchronously) "sync" else "async")
  props.put("metadata.broker.list", brokerList)
  props.put("batch.num.messages", batchSize.toString)
  props.put("message.send.max.retries", messageSendMaxRetries.toString)
  props.put("request.required.acks",requestRequiredAcks.toString)
  props.put("client.id",clientId.toString)

  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))
  
  def kafkaMesssage(message: Array[Byte], partition: Array[Byte]): KeyedMessage[AnyRef, AnyRef] = {
     if (partition == null) {
       new KeyedMessage(topic,message)
     } else {
       new KeyedMessage(topic,partition,message)
     }
  }
  
  def send(message: String, partition: String = null): Unit = send(message.getBytes("UTF8"), if (partition == null) null else partition.getBytes("UTF8"))

  def send(message: Array[Byte], partition: Array[Byte]): Unit = {
    try {
      producer.send(kafkaMesssage(message, partition))
    } catch {
      case e: Exception =>
        e.printStackTrace
    }        
  }
  
  def main(args:Array[String]){
    send("xxxxxxxxxxxxxxxxxxx")
  }
}