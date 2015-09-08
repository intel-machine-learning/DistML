#!/bin/bash 

DIR=`dirname "$0"`

. $DIR/conf.sh

CLASSPATH=/home/spark/projects/spark-1.4/assembly/target/scala-2.10/spark-assembly-1.4.2-SNAPSHOT-hadoop2.2.0.jar

echo "master: $SPARK_MASTER"
echo "jars: $APP_JARS" 
echo "inputData: $INPUT_DATA"
echo "output: $OUTPUT" 
echo "classpath: $CLASSPATH" 
echo "apppath: $APP_JARS" 

MainClass="com.intel.distml.app.rosenblatt.RosenblattApp"
if [ "$1" = "mnist" ]; then
	MainClass="com.intel.distml.app.mnist.MNIST"
elif [ "$1" = "word2vec" ]; then
	MainClass="com.intel.distml.app.word2vec.EngWords"
elif [ "$1" = "lda" ]; then
	MainClass="com.intel.distml.app.lda.LDA"
elif [ "$1" = "mini20" ]; then
	MainClass="com.intel.distml.app.lda.Mini20"
fi

echo ""
echo "Run job with $MainClass"
echo ""

start_time=`date +%s`
java $SPARK_JAVA_OPTS -cp $CLASSPATH:$APP_JARS $APP_JVM_CONFIG $MainClass $SPARK_MASTER $SPARK_HOME $SPARK_MEM $APP_JARS 
end_time=`date +%s`
echo "job duration: `expr $end_time - $start_time`"
