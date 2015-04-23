Introduction
=================
  DistML(Distributed Machine Learning Platform) is a machine learning tool which allows traing very large models on Spark or Hadoop.

  Currently DistML has following features:
  1. Independent to Spark or Hadoop, can run with mllib/mahout inside of same application
  2. Support big models across multiple nodes
  3. long based key and DMatrix based data descriptor provide high efficience and flexibility
  4. Carefully designed API to reduce program complexity.
  5. Kryo-based serializor provides high transport efficence.
  6. Fine control to training processes, support iterative training, ASGD and mini-batch training
  7. Auto and cutomizable partitioning method on model parameters.
  8. Sample alogirithms/apps provided. (Logistic Regression, Word2Vec, MNIST ...)


Data Parallel and Model Parallel
-----------------
  Most modern data analytics platform, such as VW, mahout on Hadoop, mllib on Spark, support data parallel to handle huge dataset. Generally they use a worker cluster and a master (¡°driver¡± in Spark), when start training, master sends the model to all works (by ¡°broadcast¡± in Spark), then each worker fits the model with its own part of dataset. After all works finish training, master will aggregate all updated models from workers. If several iterations needed, master sends out the aggregated model and start new iteration of training.
![image](https://github.com/intel-machine-learning/DistML/blob/master/doc/data_parallel.png)

  However in industry problems, we often need to handle very big models. For example, logistic regress may need 1~10 billion features. Hosting these features in a single server (master) or one worker is not realistic. With model parallel, these parameters are stored in a server cluster, and workers are also grouped to host necessary parameters in each group, the group size can be one or more. Like data parallel, each worker group updates the parameters with local dataset, then the updates are pushed to server and merged to existed parameters.
![image](https://github.com/intel-machine-learning/DistML/blob/master/doc/model_parallel.png)

  Notes: parameter updates from worker to server may occur asynchronously, or only occur at the end of each iteration, this depends on platform/algorithm implementation. Mllib on spark chooses the last solution because of RDD limitions.
  
Distributed Machine Platform
-----------------
  The architect of Distributed Machine Learning Platform is below:
![image](https://github.com/intel-machine-learning/DistML/blob/master/doc/architect.png)


DistML Api for Algorithms
=================

DMatrix, Matrix, Partition and KeyCollection
-----------------
  All Machine Learning problems involve a model, which contains some parameters, computing data and formulas to update the parameters. In a model parallel training system, the parameters and some of computing data have to be put in several machines. DMatrix is a concept to represent the distributed parameters or data.
  DMatrix is only a descriptive concept, it defines the matrix name, how it is partitioned, how it is initialized on server or worker.
  Since each machine contains part of DMatrix, and same to data transferring between machines, we use ¡°Matrix¡± to represent ¡°real¡± part of DMatrix data. Matrix is an abstract class indicating a 0~N dimension data block whose elements are customizable. 
  DMatrix is partitioned by its first dimension ¡°row¡±, so each Matrix knows its row key list or row key range. Matrix has an optional second dimension ¡°column¡±, which provide flexibility in selective data transferring.

Model
-----------------
  Model is an abstract class in which you can specify needed parameters, computing data and computations on samples. The main functions of a Model are as below:
  void registerMatrix(String name, DMatrix dmatrix);
  Register a DMatrix to the model.
  
  void preTraining(int workerIndex, DataBus databus);
  What to do before training, you can choose to fetch the parameters before the training, or choose to do nothing, so that you can fetch needed parameters just before training on samples.
  
  void postTraining(int workerIndex, DataBus databus);
  What to do after training complete in a worker, for example, push accumulated updates to parameter servers.
  
  void compute(Matrix samples, int workerIndex, DataBus databus);
  The main training function. Generally you can compute updates to the parameters according to the sample.
  
  void	progress(long totalSamples, long progress, MonitorDataBus dataBus)  This function is called with the progress of training. 

NeuralNetwork Models
-----------------
  Neural networks is very common is machine learning problems, one disadvantage of mllib is that it doesn¡¯t support neural network even in latest versions. As a supplement, DistML provide some API to make it easy, even for very big network models(though very rare).
![image](https://github.com/intel-machine-learning/DistML/blob/master/doc/neural_network.png)

  NeuralNetwork is represented by several layers, and there can be one or more edges between two layers. In training, the network is computed layer-by-layer(forward) and parameters are adjusted  accordingly(backward).

  With edge concept, you can define a very complex neuralnetwork, it¡¯s out of this document¡¯s scope.

DataBus
-----------------
>ServerDataBus
  Before training or in training, workers need to fetch parameters from servers, as a result, workers need to push parameter updates or updated parameters back to the servers. In DistML, this work can be easily done by ¡°DataBus¡±. In most cases, only ¡°ServerDataBus¡± is needed for parameter fetching and pushing. ServerDataBus provides following interfaces:
  Matrix fetchFromServer(String matrixName, KeyCollection rows, KeyCollection cols);
  void pushUpdates(String matrixName, Matrix update

>DataBus
  For special cases, you may need to exchange data between workers, we put these workers in a group, and partition specified matrix inside of the group, then you can retrieve data via class ¡°DataBus¡±:
  Matrix fetchFromWorker(String matrixName, KeyCollection rows, KeyCollection cols);

>MonitorDataBus
  In most cases, there are some global variables, such as learning rate, global loss value, you can store it in parameter servers, but a better way is to store in monitor, and broadcast to all workers if needed, for example, you can update learning rate in function Model.progress, then broadcast it to all workers.
  void	broadcast(String name, Object value) 
  
Training Configuration and Training Helper
-----------------
  Different algorithms need different training methods, for example, logistic regression tends to use iterative training, google DistBelief uses asynchronous stochastic downgrade and mini-batch. To be general, DistML provides support to all these training methods.
  At scale level, DistML allows user to specify the parameter server number, worker group size and number.

