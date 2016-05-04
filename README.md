# DistML (Distributed Machine Learning platform)

  DistML is a machine learning tool which allows traing very large models on Spark, it's fully compatible with Spark (tested on 1.2 or above).
  
  <img src=https://github.com/intel-machine-learning/DistML/blob/master/doc/architect.png>
  
  Reference paper: [Large Scale Distributed Deep Networks](http://research.google.com/archive/large_deep_networks_nips2012.html)
  
  
  Runtime view:
  
  <img src=https://github.com/intel-machine-learning/DistML/blob/master/doc/runtime.png>
  
  DistML provides several algorithms (LR, LDA, Word2Vec, ALS) to demonstrate its scalabilites, however, you may need to write your own algorithms based on DistML APIs(Model, Session, Matrix, DataStore...), generally, it's simple to extend existed algorithms to DistML, here we take LR as an example: [How to implement logistic regression on DistML](https://github.com/intel-machine-learning/DistML/tree/master/doc/lr-implementation.md).

### User Guide
  1. [Download and build DistML](https://github.com/intel-machine-learning/DistML/tree/master/doc/build.md).
  2. [Typical options](https://github.com/intel-machine-learning/DistML/tree/master/doc/options.md).
  3. [Run Sample - LR](https://github.com/intel-machine-learning/DistML/tree/master/doc/sample_lr.md).
  4. [Run Sample - MLR](https://github.com/intel-machine-learning/DistML/tree/master/doc/sample_mlr.md).
  5. [Run Sample - LDA](https://github.com/intel-machine-learning/DistML/tree/master/doc/sample_lda.md).
  6. [Run Sample - Word2Vec](https://github.com/intel-machine-learning/DistML/tree/master/doc/sample_word2vec.md).
  7. [Run Sample - ALS](https://github.com/intel-machine-learning/DistML/tree/master/doc/sample_als.md).
  8. [Benchmarks](https://github.com/intel-machine-learning/DistML/tree/master/doc/benchmarks.md).
  9. [FAQ](https://github.com/intel-machine-learning/DistML/tree/master/doc/faq.md).

### API Document
  1. [Source Tree](https://github.com/intel-machine-learning/DistML/tree/master/doc/src_tree.md).
  2. [DistML API](https://github.com/intel-machine-learning/DistML/tree/master/doc/api.md).


## Contributors
  He Yunlong (Intel)<br>
  Sun Yongjie (Intel)<br>
  Liu Lantao (Intern, Graduated)<br>
  Hao Ruixiang (Intern, Graduated)<br>
