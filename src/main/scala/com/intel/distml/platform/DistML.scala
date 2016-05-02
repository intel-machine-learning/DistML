package com.intel.distml.platform

import java.io.{DataInputStream, DataOutputStream}
import java.net.URI
import java.util.Properties

import akka.actor._
import com.intel.distml.api.Model
import com.intel.distml.platform.MonitorActor._
import com.intel.distml.util.DataStore
import com.intel.distml.util.scala.DoubleArrayWithIntKey
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{JavaSparkListener, SparkContext}

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.collection.JavaConversions._

/**
 * Created by yunlong on 12/8/15.
 */

object DistMLState extends Enumeration {
  type DistMLState = Value
  val READY, RECYCLED = Value

  def isReady(state : DistMLState) : Boolean = {
    state == READY
  }
}

class DistML[T: ClassTag] (
val model : Model,
val psCount : Int,
system: ActorSystem,
val monitorPath : String,
monitorActor : ActorRef,
psDriverThread : ParamServerDriver[T]
) extends JavaSparkListener
{

  var state = DistMLState.READY

  def params() : RDD[T] = {
    assertRecycled()

    psDriverThread.finalResult
  }

  def assertReady(): Unit = {
    if (!DistMLState.isReady(state)) {
      throw new IllegalStateException("parameter servers are not running.")
    }
  }

  def assertRecycled(): Unit = {
    if (DistMLState.isReady(state)) {
      throw new IllegalStateException("parameter servers are still running.")
    }
  }

  def setTrainSetSize(size : Long): Unit = {
    assertReady()

    val req = new StartTraining(size)

    monitorActor.tell(req, null)
    while (!req.done) {
      Thread.sleep(10)
    }
  }

  def startSSP(maxIterations : Int, maxLag : Int): Unit = {
    assertReady()

    val req = StartTraining.ssp(maxIterations, maxLag);

    monitorActor.tell(req, null)
    while (!req.done) {
      Thread.sleep(10)
    }
  }

  def iterationDone(): Unit = {
    assertReady()

    val req = new IterationDone()

    monitorActor.tell(req, null)
    while (!req.done) {
      Thread.sleep(10)
    }
  }

  def save(path : String): Unit = {
    assertReady()

    val req = new SaveModel(path)

    monitorActor.tell(req, null)
    while (!req.done) {
      Thread.sleep(10)
    }
  }

  def load(path : String): Unit = {
    assertReady()

    val req = new LoadModel(path)

    monitorActor.tell(req, null)
    while (!req.done) {
      Thread.sleep(10)
    }
  }

  def zero(matrixName : String): Unit = {
    assertReady()

    val req = new ZeroModel(matrixName)

    monitorActor.tell(req, null)
    while (!req.done) {
      Thread.sleep(10)
    }
  }

  def random(matrixName : String): Unit = {
    assertReady()

    val req = new RandModel(matrixName)

    monitorActor.tell(req, null)
    while (!req.done) {
      Thread.sleep(10)
    }
  }

  def init(matrixName : String, value : String): Unit = {
    assertReady()

    val req = new SetModel(matrixName, value)

    monitorActor.tell(req, null)
    while (!req.done) {
      Thread.sleep(10)
    }
  }

  def setAlpha(matrixName : String, initialAlpha : Float, minAlpha : Float, factor : Float): Unit = {
    assertReady()

    val req = new SetAlpha(matrixName, initialAlpha, minAlpha, factor)

    monitorActor.tell(req, null)
    while (!req.done) {
      Thread.sleep(10)
    }
  }

  override def onExecutorRemoved(e: SparkListenerExecutorRemoved): Unit = {
    println("executor removed: " + e.executorId)
    val req = new PSTerminated(e.executorId)
    monitorActor.tell(req, null)
  }


  def recycle(): Unit = {
    assertReady()

    monitorActor.tell(new TrainingDone(), null)

    psDriverThread.join()
    system.awaitTermination()

    state = DistMLState.RECYCLED
  }

//  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) { }
//  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) { }
//  override def onTaskStart(taskStart: SparkListenerTaskStart) { }
//  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) { }
//  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) { }
//  override def onJobStart(jobStart: SparkListenerJobStart) { }
//  override def onJobEnd(jobEnd: SparkListenerJobEnd) { }
//  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) { }
//  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) { }
//  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) { }
//  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) { }
//  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) { }
//  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) { }
//  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) { }
//  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded) { }
//  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) { }
//  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated) { }

}

object DistML {

  var ACTOR_SYSTEM_CONFIG =
    """
      |akka.actor.provider="akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.port=0
      |akka.remote.log-remote-lifecycle-events=off
      |akka.log-dead-letters=off
      |akka.io.tcp.direct-buffer-size = 2 MB
      |akka.io.tcp.trace-logging=off
      |akka.remote.netty.tcp.maximum-frame-size=4126935
    """.stripMargin

  def dummyF(model : Model, index : Int, stores : java.util.HashMap[String, DataStore]) : Int = {
    1
  }

  def defaultF(model : Model, index : Int, stores : java.util.HashMap[String, DataStore])
              : Iterator[(Int, String, DataStore)] = {

    println("collecting model parameters...")

    val map = new Array[(Int, String, DataStore)](stores.size())
    var i = 0
    for (name <- stores.keySet()) {
      val t : String = name
      map(i) = (index, t, stores.get(name))
      i += 1
    }

    println("done")
    map.iterator
  }

  def distribute(sc : SparkContext, model : Model, psCount : Int): DistML[Int] = {
    distribute[Int](sc, model, psCount, false, dummyF _)
  }

//  def distribute(sc : SparkContext, model : Model, psCount : Int, psBackup : Boolean): DistML[Int] = {
//    distribute[Int](sc, model, psCount, psBackup, dummyF)
//  }

//  def distribute[T: ClassTag](sc : SparkContext, model : Model, psCount : Int,
//                              f : Function3[Model, Int, java.util.HashMap[String, DataStore], T]): DistML[T] = {
//    distribute[T](sc, model, psCount, false, f)
//  }
  def distribute[T: ClassTag](sc : SparkContext, model : Model, psCount : Int,
                            f : Function3[Model, Int, java.util.HashMap[String, DataStore], T]): DistML[T] = {

    distribute[T](sc, model, psCount, false, f)
  }

  def distribute[T: ClassTag](sc : SparkContext, model : Model, psCount : Int, psBackup : Boolean,
                              f : Function3[Model, Int, java.util.HashMap[String, DataStore], T]): DistML[T] = {

    model.autoPartition(psCount)

    val MONITOR_ACTOR_SYSTEM_NAME = "monitor-system"
    val MONITOR_ACTOR_NAME = "monitor"

    // Start actor system
    val monitorRemoteConfig = ConfigFactory.parseString(ACTOR_SYSTEM_CONFIG)
    val monitorActorSystem = ActorSystem(MONITOR_ACTOR_SYSTEM_NAME, ConfigFactory.load(monitorRemoteConfig))

    val monitorActorRef = monitorActorSystem.actorOf(MonitorActor.props(model), MONITOR_ACTOR_NAME)

    val address = monitorActorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    model.monitorPath = monitorActorRef.path.toSerializationFormatWithAddress(address)

    var modelBroadcast = sc.broadcast(model)
    val psThread = new ParamServerDriver[T](sc, modelBroadcast, ACTOR_SYSTEM_CONFIG, model.monitorPath, psCount, psBackup, f)
    psThread.start()

    while (!model.psReady) {
      Thread.sleep(10)
    }
    println("=========== model distributed ==============");

    val dm = new DistML[T](model, psCount, monitorActorSystem, model.monitorPath, monitorActorRef, psThread)
    sc.addSparkListener(dm)
    dm
  }

  def saveMeta(hdfsPath : String, meta : Properties, comments: String) {
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(URI.create(hdfsPath), conf)

    val dst: Path = new Path(hdfsPath + "/meta.txt")
    val out: DataOutputStream = fs.create(dst)

    meta.store(out, comments)

    out.flush
    out.close
    fs.close()
  }

  def loadMeta(hdfsPath: String): Properties = {

    val props = new Properties()
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(URI.create(hdfsPath), conf)

    val dst: Path = new Path(hdfsPath + "/meta.txt")
    val in: DataInputStream = fs.open(dst)

    props.load(in)
    in.close
    fs.close()

    props
  }

}
