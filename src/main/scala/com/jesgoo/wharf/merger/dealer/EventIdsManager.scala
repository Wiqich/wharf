package com.jesgoo.wharf.merger.dealer

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import com.jesgoo.wharf.core.config.WharfContext
import com.jesgoo.wharf.main.Merger
import scala.collection.mutable.ListBuffer
class EventIdsManager extends Runnable{
  val id_map = new ConcurrentHashMap[String,Long]()
  
  val retain : Long = Merger.context.EVENT_ID_MANAGER_RETAIN
  val period : Long = Merger.context.EVENT_ID_MANAGER_CLEAR_PERIOD
  
  val size : Long = Merger.context.EVENT_ID_MANAGER_CLEAR_SIZE
  var isRun = false;
  
  def putdata(id:String) = id_map.put(id, System.currentTimeMillis())
    
  def length() : Int = id_map.size
  
  def clearAll() = id_map.clear()
  
  def remove(id:String) = id_map.remove(id)
  
  def clear(){
    val cur_time = System.currentTimeMillis()
    val ks = id_map.keys()
    val l = new ListBuffer[String]
    while(ks.hasMoreElements()){
      val key = ks.nextElement()
      if((cur_time - id_map.get(key)) > period){
        l.+=(key)
      }
    }
    for(key <- l){
      id_map.remove(key)
    }
  }
  
  def isHas(key:String) : Boolean = {
    val ishas = id_map.contains(key)
    if(ishas){
       id_map.put(key, System.currentTimeMillis())
    }
    ishas
  }
  
  def stop(){
     isRun =false
     clearAll
  }
  
  def run(){
    isRun =true;
    while(isRun){
      if(id_map.size() > size){
          clear()
      }
      Thread.sleep(retain)
    }
  }
}