package com.jesgoo.wharf.merger.puller

import com.jesgoo.wharf.thrift.wharfdata.Event

trait Puller extends Runnable{
    def pull(evt:Event):Boolean
    def stop()
}