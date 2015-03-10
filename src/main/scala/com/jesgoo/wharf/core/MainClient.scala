package com.jesgoo.wharf.core

import com.jesgoo.wharf.core.client.WharfConnClient

object MainClient {
  def main(args:Array[String]){
      val client = new WharfConnClient(8990)
      println("server has run")
      println(client.ping())
    }
}