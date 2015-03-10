package com.jesgoo.wharf.worker.getter

import com.jesgoo.wharf.worker.hamal.Hamal

trait Getter extends Runnable{
  
  def setHamal(hamal : Hamal)
  
  def stop()
  
}