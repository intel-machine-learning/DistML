#!/bin/bash

DIR=`dirname "$0"`

# spark related settings

export SCALA_HOME=/home/spark/scala-2.10.4
SPARK_MASTER=spark://dl-s1:7077
#SPARK_MASTER=local[2]
LOCAL_SPARK_HOME=/home/spark/projects/spark-1.2-yarn
SPARK_HOME=/home/spark/projects/spark-1.2-yarn
SPARK_MEM=5g # heap size for spark working instance, should be not larger than SPARK_WORKER_MEMORY in spark-env.sh
export SPARK_MEM=5g

# export SPARK_JAVA_OPTS="$SPARK_JAVA_OPTS -XX:+HeapDumpOnOutOfMemoryError -XX:+UseParallelGC -XX:ParallelGCThreads=4 -XX:+UseTLAB -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/clustering.g -Dspark.storage.memoryFraction=0.2 -Dspark.cleaner.ttl=3600000" # app configuration passed to Spark

#APP_JARS=target/distml-0.1-jar-with-dependencies.jar
APP_JARS=target/distml-0.1.jar

