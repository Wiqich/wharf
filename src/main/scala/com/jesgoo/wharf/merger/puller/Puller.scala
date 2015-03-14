package com.jesgoo.wharf.merger.puller

import com.jesgoo.wharf.thrift.wharfdata.Event

trait Puller extends Runnable{
    var mystatus = true
    def pull(evt:Event):Boolean
    def stop()
}