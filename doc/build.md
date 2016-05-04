
## Guide to download and build DistML


### Download DistML
```sh
  $ git clone https://github.com/intel-machine-learning/DistML.git
```

### Configure and build
You may use a Spark with different version as DistML, though DistML is compatible with recent Spark releases, we encourage you to build DistML with same version Spark as that you are using.
```sh
  $ cd DistML
  $ vi pom.xml
  $ mvn package
```

After successful building, you will see target/distm-<ver>.jar, refer guides to run samples.
