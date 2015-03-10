package com.jesgoo.wharf.worker.getter

import com.jesgoo.wharf.core.Data
import com.jesgoo.wharf.worker.hamal.Hamal
import scala.sys.process._
import com.jesgoo.wharf.core.config.LOG

class TailFileGetter(file : String) extends Getter{
  private var hamal : Hamal = null
  private val cmd = Seq("tail","-n 0","-F",file)
  private var it : Iterator[String] = null
  private val name = file.substring(file.lastIndexOf("/")+1);
  
  def setHamal(hamal : Hamal) {
    this.hamal = hamal
  }
  
  def init(){
    try{
        val strp = cmd.lineStream
        it = strp.iterator
    }catch {
      case e:Exception =>
      e.printStackTrace()
      LOG.error("TailFileGetter:file ",file,e.getMessage)
      it = null
    }
    
  }
  def run(){
    LOG.debug("TailFileGetter start with filename ",file)
    init()
    while(it == null){
      LOG.error("TailFileGetter:file not init ok reinit , sleep 10s")
      init()
      Thread.sleep(5000)
    }
    if(it != null){
      var tmpstr =""
      while(it.hasNext){
        tmpstr = it.next()
        //LOG.debug("TailFileGetter: get data is ",tmpstr)
        val data = new Data(tmpstr)
        data.setFilename(name)
        //LOG.debug("TailFileGetter: put data to Hamal; data = ",data.toString())
        hamal.in(data)
      }
    }
  }
  
  def stop(){
  }
}