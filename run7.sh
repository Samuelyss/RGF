#!/bin/bash
java -server -d64 -classpath `ls ~/rgf/lib/*.jar |tr "\n" ":"` nc.GridNcEleH "201902182030" "201902191650"&
#java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=2345 -Xmx100g -classpath `ls ~/calf/lib/*.jar |tr "\n" ":"` amtf.Scheduler
