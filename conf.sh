#!/bin/bash

DIR=`dirname "$0"`

# spark related settings

export SCALA_HOME=/home/spark/scala-2.10.4
#SPARK_MASTER=spark://dl-s1:7077
SPARK_MASTER=local[2]
LOCAL_SPARK_HOME=/home/spark/projects/spark-1.2-yarn
SPARK_HOME=/home/spark/projects/spark-1.2-yarn
#SPARK_MEM=20g # heap size for spark working instance, should be not larger than SPARK_WORKER_MEMORY in spark-env.sh
export SPARK_MEM=0g

export SPARK_JAVA_OPTS="$SPARK_JAVA_OPTS -Xmx5g -Xms5g -Xmn2g -XX:+UseParallelGC -XX:ParallelGCThreads=4 -XX:+PrintGCTimeStamps -XX:+PrintGCDetails -XX:MaxPermSize=1g " 
# export SPARK_JAVA_OPTS="$SPARK_JAVA_OPTS -XX:+HeapDumpOnOutOfMemoryError -XX:+UseParallelGC -XX:ParallelGCThreads=4 -XX:+UseTLAB -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/clustering.g -Dspark.storage.memoryFraction=0.2 -Dspark.cleaner.ttl=3600000" # app configuration passed to Spark

#-Xmx2000M -Xms2000M -Xmn500M -XX:PermSize=250M -XX:MaxPermSize=250M -Xss256K -XX:+DisableExplicitGC -XX:SurvivorRatio=1 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=0 -XX:+CMSClassUnloadingEnabled -XX:LargePageSizeInBytes=128M -XX:+UseFastAccessorMethods -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=60 -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+PrintClassHistogram -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC -Xloggc:log/gc.log 


#APP_JARS=target/distml-0.1-jar-with-dependencies.jar
APP_JARS=target/distml-0.1.jar

