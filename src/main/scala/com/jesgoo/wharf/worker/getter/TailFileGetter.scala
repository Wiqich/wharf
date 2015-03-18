package com.jesgoo.wharf.worker.getter

import com.jesgoo.wharf.core.Data
import com.jesgoo.wharf.worker.hamal.Hamal
import scala.sys.process._
import com.jesgoo.wharf.core.config.LOG
import org.apache.log4j.Logger

class TailFileGetter(file: String) extends Getter {
  private var hamal: Hamal = null
  private val cmd = Seq("tail", "-n 0", "-F", file)
  private var it: Iterator[String] = null
  private val name = file.substring(file.lastIndexOf("/") + 1);
  val logger = Logger.getLogger(getClass.getName)

  var isRun = false
  def setHamal(hamal: Hamal) {
    this.hamal = hamal
  }

  def init() {
    try {
      val strp = cmd.lineStream
      it = strp.iterator
    } catch {
      case e: Exception =>
        e.printStackTrace()
        LOG.warn(logger, "TailFileGetter:file ", file, e.getMessage)
        it = null
    }

  }
  def run() {
    mystatus = true
    isRun = true
    LOG.debug(logger, "TailFileGetter start with filename ", file)
    init()
    while (it == null && isRun) {
      LOG.warn(logger, "TailFileGetter:file not init ok reinit , sleep 2s")
      init()
      Thread.sleep(2000)
    }
    if (it != null) {
      var tmpstr = ""
      LOG.info(logger, "tail -F", file, "ing")
      while (it.hasNext && isRun) {
        try {
          tmpstr = it.next()
          val data = new Data(tmpstr)
          data.setFilename(name)
          hamal.in(data)
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    }
  }

  def stop() {
    isRun = false
  }
}