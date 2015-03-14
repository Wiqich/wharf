package com.jesgoo.wharf.merger.dealer

import scala.collection.mutable.HashMap
import org.apache.log4j.Logger

import com.jesgoo.wharf.core.config.LOG
import com.jesgoo.wharf.merger.puller.FilePuller

class DealerManager extends Runnable{
   
  val dealers = new HashMap[String,Dealer]
  
  val logger = Logger.getLogger(getClass.getName)
  
  var isRun = false
  def addDealer(key:String,port:Int):Boolean = {
    try{
      val dealer = new ThriftEventDealer(port)
      val pull = new FilePuller()
      dealer.setPuller(pull)
      val pull_thd = new Thread(pull)
      pull_thd.start
      Thread.sleep(1000)
      val dealer_thd = new Thread(dealer)
      dealer_thd.start
      dealers(key) = dealer
    }catch{
      case ex: Exception => 
        LOG.error(logger,"DealerManager add dealer fail",key)
        ex.printStackTrace()
        false
    }
    LOG.debug(logger,"DealerManager add dealer success",key)
    true
  }
  
  def delDealer(key:String){
    dealers(key).stop
    dealers -= key
    LOG.debug(logger,"DealerManager delete dealer ",key)
  }
  
  def recover(key:String){
    dealers(key).stop
    LOG.debug(logger, "stop ",key,"and start a new server")
    new Thread(dealers(key)).start
  }
  
  def run(){
    isRun = true
    while(isRun){
      for(tmps <- dealers.keySet){
        if(dealers(tmps).mystatus == false){
          recover(tmps)
        }
      }
      Thread.sleep(1000)
    }
  }
}