package com.jesgoo.wharf.core

class Data(data : String) {
    var filename : String = ""
    val time = System.currentTimeMillis()
    
    def setFilename(name:String) = {
        filename = name 
    }
    def getTime() :Long = time
    def getData() :String = data
    
    override def toString():String = filename +","+time+","+data
    
}