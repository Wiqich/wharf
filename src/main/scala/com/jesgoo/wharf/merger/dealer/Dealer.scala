package com.jesgoo.wharf.merger.dealer

import com.jesgoo.wharf.merger.puller.Puller

trait Dealer extends Runnable{

  def setPuller(p:Puller)
  
  def stop()

}