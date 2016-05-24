
== sample command to run lightlda

```sh
#./spark-submit --class com.intel.distml.example.LightLDA --master yarn://dl-s1:8088 /home/spark/yunlong/distml-0.2.jar --psCount 1 --maxIterations 100 --k 2 --alpha 0.01 --beta 0.01 --batchSize 100 /data/lda/short.txt
```

== About LDA parameters:
```
k: topic number, default value 20
alpha : hyper parameter, default value 0.01
beta : hyper parameter, default value 0.01
showPlexity: whether to show perplexity after training, default value true
```
