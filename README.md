Introduction
=================
  DistML(Distributed Machine Learning Platform) is a machine learning tool which allows traing very large models on Spark or Hadoop.

Run Examples
-----------------
  $ mvn package<br>
  $ cd ${SPARK}/bin<br>
  $ ./spark-submit --class com.intel.distml.example.SparseLR --master yarn://dl-s1:8088 ${DISML}/target/distml-0.1.jar --maxIterations 10 hdfs://dl-s1:9000/data/lr/Blanc-Mel.txt<br>
  $ ./spark-submit --class com.intel.distml.example.LightLDA --master yarn://dl-s1:8088 /home/spark/yunlong/distml-0.2.jar --psCount 1 --maxIterations 100 --k 1000 --alpha 0.01 --beta 0.01 --batchSize 1000 --showPlexity false /data/lda/nips_train.txt<br>

Contributors
-----------------
  He Yunlong (Intel)<br>
  Sun Yongjie (Intel)<br>
  Liu Lantao (Intern, Graduated)<br>
  Hao Ruixiang (Intern, Graduated)<br>
