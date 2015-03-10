package com.jesgoo.wharf.main

import com.jesgoo.wharf.worker.hamal.Hamal
import com.jesgoo.wharf.worker.getter.Getter
import com.jesgoo.wharf.worker.pusher.Pusher

class Driver(name:String){
    var getter : Getter = null
    var hamal : Hamal = null
    var pusher : Pusher = null

    val dName = name
    
    val period  = Worker.context.DRIVER_PERIOD
    
    def init(g:Getter,h:Hamal,p:Pusher){
      getter = g
      hamal = h
      pusher = p
    }
    
    def todo(){
      new Thread(hamal).start()
      Thread.sleep(1000)
      new Thread(getter).start()
      new Thread(pusher).start()
    }
    
    def delete(){
      if (getter != null)
        getter.stop
      if(pusher != null){
        pusher.stop
       if(hamal != null)
         hamal.stop
      }
    }
}