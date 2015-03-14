package com.jesgoo.wharf.worker.pusher

import com.jesgoo.wharf.worker.hamal.Hamal

trait Pusher extends Runnable{
  
  var mystatus = true
  
  def setHamal(hamal: Hamal)
  
  def stop()
}