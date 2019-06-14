#!/bin/bash
java -classpath `ls ~/calf270/lib/*.jar |tr "\n" ":"` dict.Init
#java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=2345 -Xmx100g -classpath `ls ~/calf/lib/*.jar |tr "\n" ":"` amtf.Scheduler
