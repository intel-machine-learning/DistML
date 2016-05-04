
## DistML source tree reading guide
Though only supports Spark now, DistML is designed to be runnable on Spark or on Yarn directly. so we implemented main engine with Java, Spark related support and sample algorithms are written in scala.

--src
    main
      java
        com
          intel
            distml
              api
              platform
              util
      scala
        com
          intel
            distml
              clustering
              example
              feature
              platform
              regression
              util
              
