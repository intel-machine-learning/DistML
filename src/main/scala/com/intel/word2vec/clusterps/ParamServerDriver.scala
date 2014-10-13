package com.intel.word2vec.clusterps

import java.util.Date
import org.apache.hadoop.fs.{FSDataOutputStream, PathFilter, Path, FileSystem}
import org.apache.spark.rdd.{UnionRDD, RDD}
import org.apache.spark.storage.StorageLevel
import scala.Double
import scala.Long
import scala.Predef._
import scala.collection.mutable
import scala.collection.mutable.{LinkedList, HashSet, HashMap}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark._
import scala.util.Random
import SparkContext._
import java.net._
import java.io.{DataOutputStream, ObjectInputStream, DataInputStream}
import com.intel.word2vec.common.{IOHelper, FloatOps, Utils}
import org.apache.hadoop.conf.Configuration


/**
 * Created by He Yunlong on 4/24/14.
 */
object ParamServerDriver {

  @transient var trainingDriverOS : DataOutputStream = null

  var wordTree : WordTree = null

  var servers = new Array[ServerInfo](Constants.PARAM_SERVER_COUNT)
  var serverListeners = new Array[ParamServerListener](Constants.PARAM_SERVER_COUNT)

  var trainedWords = 0L
  var totalWords = 0L

  var activeWorkerCount = 0

  var outputFolder : String = null
  var modelSaved = false
  var ss : ServerSocket = null
  var serverStarter : ParamServerStarter = null

  def stopServersAndSaveModel() {
    var conf = new Configuration()
    val fs = FileSystem.get(URI.create(outputFolder), conf)
    var dst = new Path(outputFolder + "/model.bin")
    val out = fs.create(dst)

    var str = "" + wordTree.vocabSize + " " + Constants.MODEL_DIMENSION + "\n"
    out.write(str.getBytes("UTF-8"))

    for (i <- 0 to Constants.PARAM_SERVER_COUNT-1) {
      var l = serverListeners(i)
      l.collectData(out)
      l.join()
    }

    out.close()

    println("model saved as: " + outputFolder + "/model.bin")
    modelSaved = true
    ss.close()
  }

  class ReportCollector(socket : Socket) extends Thread {

    var partitionIndex = 0
    var dis = new DataInputStream(socket.getInputStream())

    activeWorkerCount += 1

    def workerDone() {
      this.synchronized{
        activeWorkerCount -= 1
        if (activeWorkerCount == 0) {
          println("\n======== All workers have finished their job ======")
          stopServersAndSaveModel()
        }
      }
    }

    override def run()  {
      partitionIndex = IOHelper.readInt(dis)

      var clientWorkDone = false
      while(!clientWorkDone) {
        while(dis.available() < 8) {
          Thread.sleep(100)
        }
        val trained = dis.readLong()
        if (trained > 0) {
          ParamServerDriver.synchronized(
            trainedWords += trained
          )
          println("worker " + socket.getInetAddress.getHostName + "." + partitionIndex + ", trained=" + trained)
          println("training progress: " + trainedWords + "/" + totalWords + ", " + (trainedWords * 100)/totalWords + "%")
        }
        else {
          clientWorkDone = true
        }
      }

      println("worker " + socket.getInetAddress.getHostName + " finished its job")
      dis.close()
      socket.close()

      workerDone()
    }
  }

  var initCount = 0
  def initDone(index : Int) {
    println("parameter server ready: " + index)

    initCount += 1
    if (initCount == Constants.PARAM_SERVER_COUNT) {
      trainingDriverOS.writeInt(Constants.PARAM_SERVER_COUNT)
      for (s <- servers) {
        trainingDriverOS.writeUTF(s.address)
        trainingDriverOS.writeInt(s.serverIndex)
        trainingDriverOS.writeInt(s.pushServicePort)
        trainingDriverOS.writeInt(s.fetchServicePort)
      }
    }
  }

  class ParamServerListener(index : Int, s : Socket) extends Thread {

    private var running = true
    private var out : FSDataOutputStream = null

    def collectData(out : FSDataOutputStream) {
      this.out = out
      running = false
    }

    override def run() {
      val dis = new DataInputStream(s.getInputStream())
      val dos = new DataOutputStream(s.getOutputStream())
      var initResult = IOHelper.readInt(dis)
      println("init done: " + initResult)
      initDone(index)

      while (running) {
        Thread.sleep(100)
      }

      println("all workers are done, notify param server...")
      dos.write(1)

      var wordIndex = IOHelper.readInt(dis)
      var dataBytes = new Array[Byte](Constants.MODEL_DIMENSION * 4)
      while (wordIndex >= 0) {
          val w = wordTree.getWord(wordIndex)
          val str = w.name + " "
          out.write(str.getBytes("UTF-8"))

          while (dis.available() < Constants.MODEL_DIMENSION * 4) {
            Thread.sleep(10)
          }

          dis.read(dataBytes)
          out.write(dataBytes)
          out.writeByte(0x0a)

        wordIndex = IOHelper.readInt(dis)
      }
    }
  }

  class ParamServerStarter(spark : SparkContext, s : Socket, paramServerNetworkPrefix : String) extends Thread {
    override def run() {
      var dis = new DataInputStream(s.getInputStream)
      totalWords = dis.readLong()

      println("reading model created by training workers")
      println("reading model created by training workers")
      var trainingDriverIS = new ObjectInputStream(s.getInputStream)
      trainingDriverOS = new DataOutputStream(s.getOutputStream)
      wordTree = trainingDriverIS.readObject().asInstanceOf[WordTree]

      val partitionSize = (wordTree.nodeCount() + Constants.PARAM_SERVER_COUNT-1) / Constants.PARAM_SERVER_COUNT
      for (i <- 0 to Constants.PARAM_SERVER_COUNT - 1) {
        servers(i) = new ServerInfo()
        servers(i).serverIndex = i
      }
      println("model initialized: " + wordTree.vocabSize)

      var da = new Array[String](Constants.PARAM_SERVER_COUNT)
      for (i <- 0 to Constants.PARAM_SERVER_COUNT - 1)
        da(i) = paramServerNetworkPrefix

      val data = spark.parallelize(da, Constants.PARAM_SERVER_COUNT)
      val dummy = data.map(paramServerTask(Utils.getLocalIP, wordTree.vocabSize)).collect()
      print("parameter servers finish their work."  + dummy.size)
    }
  }

  def paramServerTask(driverAddr : String, vocabSize : Int)(value: String) : Int = {

    println("starting server task")
    val s = new Server(driverAddr, value, vocabSize)

    s.start()

    println("stopping server task")
    1
  }

  def start(spark : SparkContext, paramServerNetworkPrefix : String) {
    ss = new ServerSocket(Constants.MONITOR_PORT)
    ss.setSoTimeout(10000)

    var count = 0
    modelSaved = false
    while (!modelSaved) {
      try {
        val s: Socket = ss.accept()

        val dis = new DataInputStream(s.getInputStream())
        var t = IOHelper.readInt(dis)
        if (t == Constants.NODE_TYPE_DRIVER) {
          println("training workers are ready, initialize parameter model....")
          serverStarter = new ParamServerStarter(spark, s, paramServerNetworkPrefix)
          serverStarter.start()
        }
        else if (t == Constants.NODE_TYPE_WORKER) {
          println("worker from " + s.getInetAddress.getHostName + " connected")
          (new ReportCollector(s)).start()
        }
        else {
          val dis = new DataInputStream(s.getInputStream())
          val dos = new DataOutputStream(s.getOutputStream())

          val server = servers(count)
          //server.address = s.getInetAddress.getHostName
          server.address = IOHelper.readString(dis)
          server.pushServicePort = IOHelper.readInt(dis)
          server.fetchServicePort = IOHelper.readInt(dis)
          println("parameter server " + count + " from " + server.address +
              ", index: " + server.serverIndex +
              ", push port: " + server.pushServicePort + ", fetch port: " + server.fetchServicePort)

          dos.writeInt(server.serverIndex)

          serverListeners(count) = new ParamServerListener(count, s)
          serverListeners(count).start()

          count += 1
        }
      }
      catch {
        case toe: SocketTimeoutException =>
        case se : SocketException =>
      }
    }

    serverStarter.join()
    println("Exiting......")
  }

  def main(args: Array[String]) {

    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }

    val opts = new scala.collection.mutable.HashMap[String, String]

    var sparkMaster = args(0)
    var sparkHome = args(1)
    var sparkMem = args(2)
    var appJars = args(3)
    outputFolder = args(4)
    var paramServerNetworkPrefix = args(5)

    IOHelper.deleteHDFS(outputFolder + "/result")

    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("Word2Vec-PS")
      .set("spark.executor.memory", sparkMem)
      .set("spark.home", sparkHome)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.driver.port", "34225")
      .set("spark.local.dir", "/mnt/disk1/spark")
      .setJars(Seq(appJars))
    val spark = new SparkContext(conf)

    start(spark, paramServerNetworkPrefix)

    spark.stop()
  }
}
