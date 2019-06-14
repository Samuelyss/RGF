#!/bin/bash
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
export  SPARK_MASTER_IP=127.0.0.1
export  SPARK_LOCAL_IP=127.0.0.1
java -server -d64 -Xms20g -Xmx31g -XX:+UseCompressedOops -XX:+UseCompressedClassPointers -XX:+UseRTMLocking  -XX:+UseParallelGC -XX:+UseParallelOldGC  -classpath `ls ~/rgf/lib/*.jar |tr "\n" ":"` amtf.MendScheduler 30 &
# nohup java -classpath `ls ~/hxf/lib/*.jar |tr "\n" ":"` amtf.Scheduler &
# tail -F nohup.out
# java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=2345 -Xmx100g -classpath `ls ~/hxf/lib/*.jar |tr "\n" ":"` amtf.Scheduler
