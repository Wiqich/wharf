package com.jesgoo.wharf.core

import com.jesgoo.wharf.core.server.WharfConnServer
import com.jesgoo.wharf.thrift.wharfconn.WharfConnService
import com.jesgoo.wharf.thrift.wharfconn.WharfConnService.Iface
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.text.SimpleDateFormat
import java.util.Date
import com.jesgoo.wharf.core.config.Utils

object Main {
 def main(args:Array[String]){
      println("hello")
      for(re <- List("12134","33","yans")){
        println(re)
      }
//      val wci = new WharfConnImpl()
//      val conn_server = new WharfConnServer(8990)
//      conn_server.init(wci)
//      conn_server.run()
//      println("server has run")
      val writeLock =  new ReentrantLock();
      
       println(mk_file_tail_name(1425567626))
       
       val str2 :String = "yangshunbo\n"
       println(Utils.md5(str2))
       val id = "sdfsdfsdfsd#123456789#iusdhnfinsdf"
       println(id.substring(id.indexOf("#") + 1,id.lastIndexOf("#")))
    }
    val minuteFormat = new SimpleDateFormat("yyyyMMddHH")
    def mk_file_tail_name(ts:Long):String={
       var t = ts
       if(String.valueOf(t).length() < 13){
        t = t * 1000
    }
    val s = minuteFormat.format(t)
    s+"0000"
  }
}