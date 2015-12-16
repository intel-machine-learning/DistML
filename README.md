Introduction
=================
  DistML(Distributed Machine Learning Platform) is a machine learning tool which allows traing very large models on Spark or Hadoop.

Run Examples
-----------------
  $ mvn package
  $ cd ${SPARK}/bin
  $ ./spark-submit --class com.intel.distml.example.SparseLR --master yarn://dl-s1:8088 ${DISML}/target/distml-0.1.jar --maxIterations 10 hdfs://dl-s1:9000/data/lr/Blanc-Mel.txt

Contributors
-----------------
He Yunlong
Liu Lantao
Hao Ruixiang
