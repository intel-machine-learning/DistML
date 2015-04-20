/** *****************************************************************************
  * Copyright 2012 Roman Levenstein
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  * *****************************************************************************/

package com.intel.distml.platform

import java.util.concurrent.{TimeUnit, ArrayBlockingQueue}
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ExtendedActorSystem}
import akka.event.Logging
import akka.serialization._
import com.esotericsoftware.kryo.io.{Output, Input}
import com.esotericsoftware.kryo.{Serializer, Kryo, Registration}
import com.intel.distml.model.word2vec.WordVectorUpdate
import com.intel.distml.model.sparselr.{WeightItem, SparseWeights}
import com.intel.distml.model.word2vec.{WordVectorWithAlpha, WordVectorUpdate}
import com.intel.distml.transport.DataBusProtocol._
import com.intel.distml.util._
import org.objenesis.strategy.StdInstantiatorStrategy

import com.esotericsoftware.kryo.util._
import com.esotericsoftware.minlog.{Log => MiniLog}

import scala.util.{Failure, Success, Try}

class KryoSerializer(val system: ExtendedActorSystem) extends akka.serialization.Serializer {
//
//  val customSerializerInitClass =
//    if (settings.KryoCustomSerializerInit == null) null
//    else
//      system.dynamicAccess.getClassFor[AnyRef](settings.KryoCustomSerializerInit) match {
//        case Success(clazz) => Some(clazz)
//        case Failure(e) => {
//          log.error("Class could not be loaded and/or registered: {} ", settings.KryoCustomSerializerInit)
//          throw e
//        }
//      }

//  val customizerInstance = Try(customSerializerInitClass.map(_.newInstance))
//
//  val customizerMethod = Try(customSerializerInitClass.map(_.getMethod("customize", classOf[Kryo])))

  // includeManifest is useless for kryo
  def includeManifest: Boolean = false

  // A unique identifier for this Serializer
  def identifier = 848200 // My student number, :)

  // Delegate to a real serializer
  def toBinary(obj: AnyRef): Array[Byte] = {
    val ser = getSerializer
    val bin = ser.toBinary(obj)
    releaseSerializer(ser)
    bin
  }

  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
    val ser = getSerializer
    val obj = ser.fromBinary(bytes, clazz)
    releaseSerializer(ser)
    obj
  }

  val serializerPool = new ObjectPool[KryoBasedSerializer](1, () => {
    new KryoBasedSerializer(getKryo(), 409600,200000000)
  })

  private def getSerializer = serializerPool.fetch

  private def releaseSerializer(ser: KryoBasedSerializer) = serializerPool.release(ser)

  private def getKryo(): Kryo = {


    val referenceResolver = new MapReferenceResolver()
    val kryo =  new Kryo(new DefaultClassResolver(), referenceResolver /*, new DefaultStreamFactory()*/)
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy())

//    val kryo: Kryo = new Kryo
    kryo.setReferences(true)
    kryo.setRegistrationRequired(true)
    kryo.addDefaultSerializer(classOf[ActorRef], new ActorRefSerializer(system))

    kryo.register(classOf[Array[Float]]);
    kryo.register(classOf[java.util.LinkedList[Long]]);
    kryo.register(classOf[java.util.HashSet[Long]]);

    kryo.register(classOf[KeyCollection.ALL_KEYS]);
    kryo.register(classOf[KeyList]);
    kryo.register(classOf[KeyRange]);

    kryo.register(classOf[PartialDataRequest]);
    kryo.register(classOf[Data]);
    kryo.register(classOf[DataList]);
    kryo.register(classOf[PushDataRequest]);
    kryo.register(classOf[java.util.LinkedList[Data]]);
    kryo.register(classOf[PushDataResponse]);

    kryo.register(classOf[WordVectorWithAlpha]);
    kryo.register(classOf[Array[WordVectorWithAlpha]]);
    kryo.register(classOf[HashMapMatrix[WordVectorWithAlpha]]);
    kryo.register(classOf[java.util.HashMap[Long, WordVectorWithAlpha]]);
    kryo.register(classOf[Matrix1D[WordVectorWithAlpha]]);
    kryo.register(classOf[WordVectorUpdate]);
    kryo.register(classOf[Array[WordVectorUpdate]]);
    //kryo.register(com.intel.scaml.transport.DataBusProtocol.PartialDataRequest.class);

    kryo.register(classOf[SparseWeights]);
    kryo.register(classOf[WeightItem]);


    kryo
  }
}

class ActorRefSerializer(val system: ExtendedActorSystem) extends Serializer[ActorRef] {

  override def read(kryo: Kryo, input: Input, typ: Class[ActorRef]): ActorRef = {
    val path = input.readString()
    system.provider.resolveActorRef(path)
  }

  override def write(kryo: Kryo, output: Output, obj: ActorRef) = {
    output.writeString(Serialization.serializedActorPath(obj))
  }
}

class KryoBasedSerializer(val kryo: Kryo, val bufferSize: Int, val maxBufferSize: Int) {

  // "toBinary" serializes the given object to an Array of Bytes
  def toBinary(obj: AnyRef): Array[Byte] = {
    println("toBinary begin: " + obj + ", " + new java.util.Date() + ", " + Thread.currentThread().getId)
    val buffer = getBuffer
    var arr: Array[Byte] = null
    try {
      println("ser begin: " + new java.util.Date());
      kryo.writeClassAndObject(buffer, obj)

      arr = buffer.toBytes()
      println("ser done: " + new java.util.Date() + ", " + arr.length);
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally
      releaseBuffer(buffer)

    println("toBinary done: " + new java.util.Date() + ", " + Thread.currentThread().getId)
    arr
  }

  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
    println("fromBinary begin: " + new java.util.Date() + ", " + Thread.currentThread().getId)
    try {
      val obj = kryo.readClassAndObject(new Input(bytes))
      println("fromBinary end: " + obj + ", " + new java.util.Date() + ", " + Thread.currentThread().getId)
      return obj
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    return null
  }

  val buf = new Output(bufferSize, maxBufferSize)

  private def getBuffer = buf

  private def releaseBuffer(buffer: Output) = {
    buffer.clear()
  }
}


class ObjectPool[T](number: Int, newInstance: () => T) {

  private val size = new AtomicInteger(0)
  private val pool = new ArrayBlockingQueue[T](number)

  def fetch(): T = {
    pool.poll() match {
      case o if o != null => o
      case null => createOrBlock
    }
  }

  def release(o: T): Unit = {
    pool.offer(o)
  }

  def add(o: T): Unit = {
    pool.add(o)
  }

  private def createOrBlock: T = {
    size.get match {
      case e: Int if e == number => block
      case _ => create
    }
  }

  private def create: T = {
    size.incrementAndGet match {
      case e: Int if e > number => size.decrementAndGet; fetch()
      case e: Int => newInstance()
    }
  }

  private def block: T = {
    val timeout = 5000
    pool.poll(timeout, TimeUnit.MILLISECONDS) match {
      case o if o != null => o
      case _ => throw new Exception("Couldn't acquire object in %d milliseconds.".format(timeout))
    }
  }
}
