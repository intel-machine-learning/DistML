
## A sample shows how write algorithms based on DistML APIs

```scala
     val m = new Model() {
      registerMatrix("weights", new DoubleArrayWithIntKey(dim + 1))
    }

    val dm = DistML.distribute(sc, m, psCount, DistML.defaultF)
    val monitorPath = dm.monitorPath

    dm.setTrainSetSize(samples.count())

    for (iter <- 0 to maxIterations - 1) {
      println("============ Iteration: " + iter + " ==============")

      val t = samples.mapPartitionsWithIndex((index, it) => {
        println("--- connecting to PS ---")
        val session = new Session(m, monitorPath, index)
        val wd = m.getMatrix("weights").asInstanceOf[DoubleArrayWithIntKey]

        val batch = new util.LinkedList[(mutable.HashMap[Int, Double], Int)]
        while (it.hasNext) {
          batch.clear()
          var count = 0
          while ((count < batchSize) && it.hasNext) {
            batch.add(it.next())
            count = count + 1
          }

          val keys = new KeyList()
          for ((x, label) <- batch) {
            for (key <- x.keySet) {
              keys.addKey(key)
            }
          }

          val w = wd.fetch(keys, session)
          val w_old = new util.HashMap[Long, Double]
          for ((key, value) <- w) {
            w_old.put(key, value)
          }

          for ((x, label) <- batch) {
            var sum = 0.0
            for ((k, v) <- x) {
              sum += w(k) * v
            }
            val h = 1.0 / (1.0 + Math.exp(-sum))

            val err = eta * (h - label)
            for ((k, v) <- x) {
              w.put(k, w(k) - err * v)
            }

            cost = cost + label * Math.log(h) + (1 - label) * Math.log(1 - h)
          }

          cost /= batch.size()
          for (key <- w.keySet) {
            val grad: Double = w(key) - w_old(key)
            w.put(key, grad)
          }
          wd.push(w, session)
        }

        session.disconnect()

        val r = new Array[Double](1)
        r(0) = -cost
        r.iterator
      })

      val totalCost = t.reduce(_+_)
      println("============ Iteration done, Total Cost: " + totalCost + " ============")
    }
```
  
## Instructions

Firstly define your model with parameter type and dimension, for logistic regression, we need a double vector, DistML provides Array/Matrix for int/long/float/double.
```scala
  val m = new Model() {
    registerMatrix("weights", new DoubleArrayWithIntKey(dim + 1))
  }
```

Before training the model, we need to distributed the parameters to several parameter server nodes, the number of parameter servers is specified by psCount.
```scala
    val dm = DistML.distribute(sc, m, psCount, DistML.defaultF)
    val monitorPath = dm.monitorPath

    dm.setTrainSetSize(samples.count())
```

In each worker doing training jobs, we need to setup a session, which helps to setup databuses between workers and parameter servers.
```scala
  val session = new Session(m, monitorPath, index)
  val wd = m.getMatrix("weights").asInstanceOf[DoubleArrayWithIntKey]
```

After connected to parameter servers, we can fetch the parameters now. Note that <i>w_old<i> is used to calculate updates after each iteration.
```scala
  val w = wd.fetch(keys, session)
  val w_old = new util.HashMap[Long, Double]
  for ((key, value) <- w) {
    w_old.put(key, value)
  }
···

With training, the parameters are updated, we calculate updates here then push to parameter servers.
```scala
  for (key <- w.keySet) {
    val grad: Double = w(key) - w_old(key)
    w.put(key, grad)
  }
  wd.push(w, session)
```
When worker finishs training of each iteration, disconnect from parameter servers.
```scala
 session.disconnect()
```
  
