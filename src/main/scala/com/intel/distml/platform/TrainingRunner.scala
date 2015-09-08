package com.intel.distml.platform

import akka.actor.{Props, ActorSystem, Actor}
import com.intel.distml.api.{ModelWriter, Model}
import com.intel.distml.platform.MonitorActor.{TrainingDone, IterationDone}
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions
import scala.reflect.ClassTag

/**
 * Created by yunlong on 9/6/15.
 */
class TrainingRunner[T:ClassTag](
spark : SparkContext,
samples: RDD[T],
modelBroadcast : Broadcast[Model],
model : Model,
monitorActorPath : String,
trainingContext : TrainingContext,
modelWriter : ModelWriter,
slave: RunnerSlave
) extends Actor with Serializable {

  val monitor = context.actorSelection(monitorActorPath)
  var tmp = samples
  var iter = 0

  monitor.tell(new MonitorActor.WorkerStarterRegister, self)

  override def receive() = {
    case value: MonitorActor.ParameterServersReady => {
      //println("parameter servers are ready, start training now. ")
      iter = 0
      slave.runIter
      //println("tell monitor iteration done.")
      monitor.tell(new IterationDone(iter), self)
    }
    case msg : MonitorActor.IterationDoneAck => {
      //println("[WorkerStarter] received: IterationDoneAck")
      iter += 1
      if (iter == trainingContext.iteration) {
        //println("tell monitor training done.")
        monitor.tell(new TrainingDone(), self)
      }
      else {
        println("**************** iteration: " + iter + " ****************")
        slave.runIter
        //println("tell monitor iteration done.")
        monitor.tell(new IterationDone(iter), self)
      }
    }
  }

  def startWorkers[T:ClassTag](spark : SparkContext, samples: RDD[T], modelBroadcast : Broadcast[Model],config : TrainingContext, modelWriter : ModelWriter) {
    println("start new iteration.");
    samples.mapPartitionsWithIndex(workerStartFunction(modelBroadcast, monitorActorPath, config)).collect
  }

  def workerStartFunction[T] (modelBroadcast : Broadcast[Model],monitorPath : String, config : TrainingContext)
                             (index : Int, samples: Iterator[T]) : Iterator[T] = {

    val results = new MyDList[T]

    // Init actor system configurations
    val WORKER_ACTOR_SYSTEM_NAME = "worker-system"
    val WORKER_LEAD_ACTOR_NAME = "worker-lead"
    val WORKER_ACTOR_NAME = "worker"

    // Start actor system
    val workerRemoteConfig = ConfigFactory.parseString(TrainingHelper.ACTOR_SYSTEM_CONFIG)
    val workerActorSystem = ActorSystem(WORKER_ACTOR_SYSTEM_NAME + index, ConfigFactory.load(workerRemoteConfig))

    val worker = workerActorSystem.actorOf(WorkerActor.props(modelBroadcast.value, monitorPath, index,
      JavaConversions.asJavaIterator(samples), config, results), WORKER_ACTOR_NAME)

    workerActorSystem.awaitTermination()

    results.iterator()
  }

}

trait RunnerSlave {
  def runIter()
}