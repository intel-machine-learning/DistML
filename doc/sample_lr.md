
== Run sparse logistic regression

```sh
#./spark-submit --num-executors 8 --executor-cores 3 --executor-memory 10g --class com.intel.distml.example.regression.MelBlanc --master yarn://dl-s1:8088 /home/spark/yunlong/distml-0.2.jar --psCount 1 --trainType ssp --maxIterations 150 --maxLag 2 --dim 1000000 --partitions 8 --eta 0.00001 /data/lr/Blanc-Mel.txt /models/lr/Blanc
```

=== Options
runType: train or test, default value: train
psCount: number of parameter servers, default value 1
psBackup : whether to enable parameter server fault toleranceBoolean = false
trainType : how to train lr model, "ssp" or "asgd", default value "ssp"
maxIterations: how many iterations to train the model, only applicable to runType=train default value 100
batchSize: batch size when using "asgd" as train type, default value 100
maxLag: max iteration difference between fastest and slowest workers, only applicable to trainType=ssp, default value 2
dim: dimension of the lr weights, default value 10000000
eta: learning rate, default value 0.0001
partitions: how many partitions for training data, default value 1
input: dataset for training, mandatory
modelPath: where to save the model, mandatory
