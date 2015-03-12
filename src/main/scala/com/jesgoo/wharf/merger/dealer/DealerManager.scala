package com.jesgoo.wharf.merger.dealer

import scala.collection.mutable.HashMap
import com.jesgoo.wharf.merger.puller.FilePuller
import com.jesgoo.wharf.core.config.LOG
import org.apache.log4j.Logger

class DealerManager{
   
  val dealers = new HashMap[String,Dealer]
  val logger = Logger.getLogger(getClass.getName)
  def addDealer(key:String,port:Int):Boolean = {
    try{
      val dealer = new ThriftEventDealer(port)
      val pull = new FilePuller()
      dealer.setPuller(pull)
      new Thread(pull).start()
      Thread.sleep(1000)
      new Thread(dealer).start()
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
    dealers(key).stop()
    dealers -= key
    LOG.debug(logger,"DealerManager delete dealer ",key)
  }
}