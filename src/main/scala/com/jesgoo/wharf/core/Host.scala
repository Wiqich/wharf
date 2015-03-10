package com.jesgoo.wharf.core

import com.jesgoo.wharf.thrift.wharfconn.Request

class Host(tmphostname:String,tmpport:Int,tmpreq:Request) {
  val hostname = tmphostname
  val port = tmpport
  val request = tmpreq

}