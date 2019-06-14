#!/bin/bash
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
# java -classpath `ls ~/hxf/lib/*.jar |tr "\n" ":"` amtf.Scheduler &
nohup java -classpath `ls ~/calf260/lib/*.jar |tr "\n" ":"` amtf.Scheduler &
tail -F nohup.out
# java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=2345 -Xmx100g -classpath `ls ~/hxf/lib/*.jar |tr "\n" ":"` amtf.Scheduler
