Introduction
=================
  DistML(Distributed Machine Learning Platform) is a machine learning tool which allows traing very large models on Spark or Hadoop.

Build and Run Examples
-----------------
<pre>
  $ git clone https://github.com/intel-machine-learning/DistML.git
  $ cd DistML
  $ mvn package
  $ cd data
  $ wget http://komarix.org/ac/ds/Blanc__Mel.txt.bz2
  $ bunzip2 Blanc__Mel.txt.bz2
  $ <copy Blanc__Mel.txt to hdfs> 

  $ cd ..
  $ export DISTML_HOME=`pwd`

  $ cd ${SPARK}/bin<br>
  $ ./spark-submit --class com.intel.distml.example.SparseLR --master yarn://dl-s1:8088 ${DISTML_HOME}/target/distml-0.2.jar --dim 2000000000 --maxIterations 10 hdfs://dl-s1:9000/data/lr/Blanc-Mel.txt
</pre>

Contributors
-----------------
  He Yunlong (Intel)<br>
  Sun Yongjie (Intel)<br>
  Liu Lantao (Intern, Graduated)<br>
  Hao Ruixiang (Intern, Graduated)<br>
