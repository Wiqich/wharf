package com.jesgoo.wharf.worker.hamal

import com.jesgoo.wharf.thrift.wharfdata.Event
import com.jesgoo.wharf.core.Data

trait Hamal extends Runnable{
  
  def in(data:Data)
  
  def out():Event
  
  def sendFinish(key: String)
  
  def stop()
  
}

