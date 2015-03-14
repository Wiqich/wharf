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
      if(hamal != null){
        new Thread(hamal).start()
      }
      Thread.sleep(1000)
      if(getter != null){
        new Thread(getter).start()
      }
      if(pusher != null){
        new Thread(pusher).start()
      }
    }
    
    def delAll(){
      delGetter
      delPusher
      delHamal
    }
    
    def delGetter(){
      if (getter != null)
        getter.stop
    }
    
    def delPusher(){
      if(pusher != null)
        pusher.stop
    }
    
    def delHamal(){
      if(hamal != null)
         hamal.stop
    }
    
    def getItemName():String = {
      dName.substring(dName.lastIndexOf("_")+1)
    }
}