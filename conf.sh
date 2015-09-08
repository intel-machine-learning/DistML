#!/bin/bash

DIR=`dirname "$0"`

# spark related settings

export SCALA_HOME=/home/spark/scala-2.10.4
SPARK_MASTER=spark://dl-s1:7078
#SPARK_MASTER=local[2]
SPARK_HOME=/home/spark/projects/spark-1.4
export SPARK_MEM=60g

export SPARK_JAVA_OPTS="$SPARK_JAVA_OPTS -Xmx60g -Xms20g -Xmn10g -XX:+UseConcMarkSweepGC -XX:+PrintGCTimeStamps -XX:+PrintGCDetails -XX:MaxPermSize=1g " 

APP_JARS=target/distml-0.1.jar

