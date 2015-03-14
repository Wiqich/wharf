package com.jesgoo.wharf.merger.puller

import java.io.File
import java.io.PrintWriter
import java.text.SimpleDateFormat
import com.jesgoo.wharf.main.Merger
import com.jesgoo.wharf.thrift.wharfdata.Event
import com.jesgoo.wharf.core.config.Utils
import com.jesgoo.wharf.thrift.wharfdata.Content
import java.util.List
import com.jesgoo.wharf.core.config.LOG
import java.io.FileWriter
import org.apache.log4j.Logger

class FilePuller extends Puller{
  
  val file_path = Merger.context.FILE_PULLER_FILEPATH
  val roll_log_type = Merger.context.FILE_PULLER_TOLL_TYPE
  val period = Merger.context.FILE_PULLER_PERIOD
  val minuteFormat = new SimpleDateFormat("yyyyMMddHH")
  
  var cur_path = ""
  var cur_name = ""
  var file_time_name : String = ""
  var writer: PrintWriter = null  
  val logger = Logger.getLogger(getClass.getName)
  
  def openWriter(evt:Event){
    if(cur_path == ""){
        cur_path = file_path+"/"+evt.head.getHostname
    }
    val file = new File(cur_path)
    if (!file.exists()) {
      file.mkdirs()
    }
    cur_name = evt.getHead.getFilename
    LOG.debug(logger,"Puller file is",cur_name," path is",cur_path)
    val op_file = new FileWriter(cur_path + "/" + cur_name, true)
    writer = new PrintWriter(op_file)
  }  
  
  def flush(){
    if(writer != null){
       writer.flush()
    }
  }
  
  def closeWriter(){
    flush
    if(writer != null){
      writer.close()
    }
    writer = null
  }
  
  def fileMv(evt:Event){
    val mv_filename = cur_path + "/" + cur_name
    var rename_filename = mv_filename+"."+file_time_name
    val f = new File(mv_filename)
    var count = 0
    while(new File(rename_filename).exists()){
      LOG.info(logger, rename_filename , "had exsit")
      rename_filename = rename_filename+"."+count
      count+=1
    }
    if(f.exists()){
      f.renameTo(new File(rename_filename))
    }else{
      println(mv_filename+" is not exsits")
    }
    
  }
  
  def pull(evt:Event):Boolean = {
    val cur_time_name = Utils.mk_file_tail_name(minuteFormat, evt.getHead.getTimestamp)
    if(file_time_name != cur_time_name){
      closeWriter
      if(cur_path != ""){
          fileMv(evt)
      }
      file_time_name = cur_time_name
      openWriter(evt)
    }
    val l_data : List[Content] = evt.body.getContents()
    LOG.debug(logger,"FilePuller put some data to=",cur_name," data length=",String.valueOf(l_data.size()))
    for(i <- 0 to l_data.size()-1){
      writer.append(l_data.get(i).getRel+"\n")
    }
    flush
    true  
  }
  
  def stop(){
    closeWriter
  }
  
  def run(){
    mystatus = true
    while(mystatus){
      Thread.sleep(period)
    }
  }
}