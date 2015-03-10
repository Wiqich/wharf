package com.jesgoo.wharf.worker.hamal

import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.util.ArrayList
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue
import scala.io.Source

import com.jesgoo.wharf.core.Data
import com.jesgoo.wharf.core.config.LOG
import com.jesgoo.wharf.core.config.Utils
import com.jesgoo.wharf.main.Worker
import com.jesgoo.wharf.thrift.wharfdata.Body
import com.jesgoo.wharf.thrift.wharfdata.Content
import com.jesgoo.wharf.thrift.wharfdata.Event
import com.jesgoo.wharf.thrift.wharfdata.EventType
import com.jesgoo.wharf.thrift.wharfdata.Head

class StoreHamal(NAME: String) extends Hamal {

  val id_count = Worker.start_time

  var cur_id = 0
  var cur_name = ""

  

  val id_map = new HashMap[String, Boolean]()
  val event_ids = new Queue[String]()

  var writer: PrintWriter = null
  val writeLock = new ReentrantLock();
  //limits
  val hamal_limit_lines = Worker.context.STORE_HAMAL_IN_LINES
  val hamal_limit_timeout = Worker.context.STORE_HAMAL_IN_TIMEOUT
  val hamal_period = Worker.context.STORE_HAMAL_PERIOD
  val cur_path = Worker.context.STORE_HAMAL_PATH
  var cur_fileName: String = NAME
  var cur_lines = 0

  def init_id_map() {
    val file_path = new File(cur_path)
    if (file_path.exists()) {
      for (file <- file_path.list()) {
        if (file.contains(NAME)) {
          LOG.debug("Store Hamal load ",file)
          id_map += (file -> true)
          event_ids += (file)
        }
      }
    }
    LOG.info("Store Hamal load finish id_map size " , String.valueOf(id_map.size) , " event_ids size " ,String.valueOf( event_ids.size))
  }

  def in(data: Data) {
    if (data.getData().trim() != "") {
      LOG.debug("Hamal in data = ",data.getData())
      writeLock.lock()
      if (writer == null) {
        openWriter(data)
      }
      writer.write(data.getData() + "\n")
      cur_lines += 1
      LOG.debug("Hamal writer file ; cur_lines = ", String.valueOf(cur_lines))
      flush
      writeLock.unlock()
    }
  }

  def flush() {
    if (writer != null) {
      writer.flush()
      LOG.debug("Hamal writer flush data")
    }
  }

  def stop() {
    writeLock.lock()
    closeWriter
    writeLock.unlock()
  }

  def run() {
    var starttime = System.currentTimeMillis()
    init_id_map
    while (true) {
      LOG.debug("Store Hamal has running name=",NAME)
      val interval_time = System.currentTimeMillis() - starttime
      if (cur_lines > hamal_limit_lines || (interval_time > hamal_limit_timeout && cur_lines > 0)) {
        LOG.debug("Store Hamal to now to close file :curline= ",String.valueOf(cur_lines),
            " interval_time= ",String.valueOf(interval_time)," limits = ",
            String.valueOf(hamal_limit_lines),";",String.valueOf(hamal_limit_timeout))
        writeLock.lock()
        this.closeWriter()
        starttime = System.currentTimeMillis()
        cur_lines = 0
        writeLock.unlock()
      }
      Thread.sleep(hamal_period)
    }
  }

  def openWriter(data : Data) {
    cur_name = id_count + "#" + data.getTime()+"#" + cur_id + "+" + cur_fileName
    cur_id += 1
    
    val file = new File(cur_path)
    if (!file.exists()) {
      file.mkdirs()
    }
    
    val op_file = new FileWriter(cur_path + "/" + cur_name, true)
    writer = new PrintWriter(op_file)
    LOG.debug("Hamal open file = "+cur_path + "/" + cur_name)
    
  }

  def closeWriter() {
    flush
    LOG.debug("Store Hamal close writer file: ",cur_name)
    if (writer != null) {
      writer.close()
    }
    event_ids += (cur_name)
    id_map += (cur_name -> true)
    writer = null
  }

  def out(): Event = {
    if (event_ids.size < 1) {
      return null
    }
    var get_id: String = event_ids.dequeue()
    LOG.debug("Store Hamal event_ids dequeue a key ",get_id)
    while (id_map.get(get_id).get != true) {
      id_map -= (get_id)
      if (event_ids.size == 0) {
        return null
      }
      get_id = event_ids.dequeue()
    }
    val content_list = new ArrayList[Content]()
    LOG.debug("Store Hamal get eventid=",get_id)
    val lines = Source.fromFile(cur_path + "/" + get_id).getLines
    var line = ""
    var md5 = ""
    while (lines.hasNext) {
      line = lines.next().trim()
      md5 = Utils.md5(line)
      val content = new Content(line, md5)
      content_list.add(content)
    }
    val head = new Head(Worker.hostname, EventType.LOG)
    head.setFilename(cur_fileName)
    head.setTimestamp(getTimeById(get_id))
    val body = new Body(content_list, content_list.size())
    val event = new Event(get_id, head)
    event.setBody(body)
    event
  }

  def sendFinish(key: String) {
    id_map.-=(key)
    val tmp_file = new File(cur_path + "/" + key)
    if (tmp_file.exists()) {
      LOG.debug("Hamal delete the file : ",tmp_file.getName) 
      tmp_file.delete()
    }else{
      LOG.error("Hamal delete the file : ",tmp_file.getName," is not exsit") 
    }
  }

  def getTimeById(id: String): Long = id.substring(id.indexOf("#") + 1,id.lastIndexOf("#")).toLong
  def getNAME():String = NAME
}