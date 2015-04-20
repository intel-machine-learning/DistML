#!/bin/bash 

DIR=`dirname "$0"`

. $DIR/conf.sh

#CLASSPATH=`bash $LOCAL_SPARK_HOME/bin/compute-classpath.sh`
#CLASSPATH=libs/spark-assembly-1.0.0-hadoop2.2.0.jar
CLASSPATH=../scaml/libs/spark-assembly-1.2.1-SNAPSHOT-hadoop2.2.0.jar

echo "master: $SPARK_MASTER"
echo "jars: $APP_JARS" 
echo "inputData: $INPUT_DATA" 
echo "output: $OUTPUT" 
echo "classpath: $CLASSPATH" 
echo "apppath: $APP_JARS" 

MainClass="com.intel.distml.app.rosenblatt.RosenblattApp"
if [ "$1" = "mnist" ]; then
	MainClass="com.intel.distml.app.mnist.MNIST"
elif [ "$1" = "mlr" ]; then
	MainClass="com.intel.distml.app.mlr.MLR"
elif [ "$1" = "word2vec" ]; then
	MainClass="com.intel.distml.app.word2vec.EngWords"
elif [ "$1" = "locallr" ]; then
	MainClass="com.intel.distml.app.lr.LR"
fi

echo ""
echo "Run job with $MainClass"
echo ""

start_time=`date +%s`
java -cp $CLASSPATH:$APP_JARS $APP_JVM_CONFIG $MainClass $SPARK_MASTER $SPARK_HOME $SPARK_MEM $APP_JARS 
end_time=`date +%s`
echo "job duration: `expr $end_time - $start_time`"
