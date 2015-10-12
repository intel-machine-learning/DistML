
wget http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz
wget http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz

gunzip train-images-idx3-ubyte.gz
gunzip train-labels-idx1-ubyte.gz

javac MNISTReader.java
java MNISTReader train-labels-idx1-ubyte train-images-idx3-ubyte mnist_train.txt
