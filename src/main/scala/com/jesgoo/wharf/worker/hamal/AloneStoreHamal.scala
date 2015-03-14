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
import org.apache.log4j.Logger

class AloneStoreHamal(NAME: String,StartTime:Long) extends Hamal {

  val id_count = StartTime

  var cur_count_id = 0
  var cur_store_name = ""
  var cur_store_tmp_name = ""

  var cur_event: Event = null

  var isRun = false
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
  
  val logger = Logger.getLogger(getClass.getName)
  
  def init_id_map() {
    val file_path = new File(cur_path)
    if (file_path.exists()) {
      for (file <- file_path.list()) {
        if (file.contains(NAME) && !file.contains("tmp")) {
          LOG.debug(logger,"Store Hamal load ",file)
          id_map += (file -> true)
          event_ids += (file)
        }
      }
    }
    LOG.info(logger, "Store Hamal load finish id_map size ", String.valueOf(id_map.size), " event_ids size ", String.valueOf(event_ids.size))
  }

  def in(data: Data) {
    if (data.getData().trim() != "") {
      LOG.debug(logger, "Hamal in data = ", data.getData())
      writeLock.lock()
      if (writer == null) {
        openWriter(data)
      }
      writer.write(data.getData() + "\n")
      cur_lines += 1
      LOG.debug(logger, "Hamal writer file ; cur_lines = ", String.valueOf(cur_lines))
      flush
      writeLock.unlock()
    }
  }

  def flush() {
    if (writer != null) {
      writer.flush()
      LOG.debug(logger, "Hamal writer flush data")
    }
  }

  def stop() {
    writeLock.lock()
    closeWriter
    writeLock.unlock()
    isRun = false
  }

  def run() {
    mystatus = true
    isRun = true
    try {
      var starttime = System.currentTimeMillis()
      init_id_map
      while (isRun) {
        //LOG.debug("Store Hamal has running name=",NAME)
        val interval_time = System.currentTimeMillis() - starttime
        if (cur_lines > hamal_limit_lines || (interval_time > hamal_limit_timeout && cur_lines > 0)) {
          LOG.debug(logger, "Store Hamal to now to close file :curline= ", String.valueOf(cur_lines),
            " interval_time= ", String.valueOf(interval_time), " limits = ",
            String.valueOf(hamal_limit_lines), ";", String.valueOf(hamal_limit_timeout))
          writeLock.lock()
          this.closeWriter()
          starttime = System.currentTimeMillis()
          cur_lines = 0
          writeLock.unlock()
        }
        if (cur_count_id > 900000000) {
          cur_count_id = 0
        }
        init_id_map
        Thread.sleep(hamal_period)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        mystatus = false
    }
  }

  def openWriter(data: Data) {
    cur_store_name = id_count + "#" + data.getTime() + "#" + cur_count_id + "+" + cur_fileName
    cur_store_tmp_name = cur_store_name+".tmp"
    
    cur_count_id += 1

    val file = new File(cur_path)
    if (!file.exists()) {
      file.mkdirs()
    }

    val op_file = new FileWriter(cur_path + "/" + cur_store_tmp_name, true)
    writer = new PrintWriter(op_file)
    LOG.debug(logger, "Hamal open file = " + cur_path + "/" + cur_store_tmp_name)

  }

  def closeWriter() {
    flush
    LOG.debug(logger, "Store Hamal close writer file: ", cur_store_tmp_name)
    if (writer != null) {
      writer.close()
    }
    val rename_file = new File(cur_store_name)
    if(rename_file.exists()){
      rename_file.delete()
    }
    
    new File(cur_store_tmp_name).renameTo(rename_file)
    
    writer = null
  }

  def out(): Event = {
    if (event_ids.size < 1) {
      return null
    }
    if (cur_event == null) {
      var get_id: String = event_ids.dequeue()
      //LOG.debug("Store Hamal event_ids dequeue a key ",get_id)
      while (id_map.get(get_id).get != true) {
        id_map -= (get_id)
        if (event_ids.size == 0) {
          return null
        }
        get_id = event_ids.dequeue()
      }
      val content_list = new ArrayList[Content]()
      //LOG.debug("Store Hamal get eventid=",get_id)
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
      cur_event = event
      event
    } else {
      cur_event
    }
  }

  def sendFinish(key: String) {
    cur_event = null
    id_map.-=(key)
    val tmp_file = new File(cur_path + "/" + key)
    if (tmp_file.exists()) {
      LOG.debug(logger, "Hamal delete the file : ", tmp_file.getName)
      tmp_file.delete()
    } else {
      LOG.error(logger, "Hamal delete the file : ", tmp_file.getName, " is not exsit")
    }
  }

  def getTimeById(id: String): Long = id.substring(id.indexOf("#") + 1, id.lastIndexOf("#")).toLong
  def getNAME(): String = NAME
}