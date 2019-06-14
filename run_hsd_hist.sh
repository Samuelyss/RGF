#!/bin/bash
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
#java -classpath `ls ~/calf230/lib/*.jar |tr "\n" ":"` amtf.Scheduler &
#java -classpath hsd1.jar amtf.Scheduler
java -classpath hsd.amtf.FtpBatch 20151011 20171012
