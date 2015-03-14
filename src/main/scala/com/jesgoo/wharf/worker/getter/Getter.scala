package com.jesgoo.wharf.worker.getter

import com.jesgoo.wharf.worker.hamal.Hamal

trait Getter extends Runnable{
  var mystatus = true
  
  def setHamal(hamal : Hamal)
  
  def stop()
  
}