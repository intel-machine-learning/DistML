package com.intel.distml.app.lda

import java.io.File

import com.intel.distml.api.Model
import com.intel.distml.model.lda.{LDAModelWriter, LDADataMatrix, Dictionary, LDAModel}
import com.intel.distml.platform.{TrainingContext, TrainingHelper}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import scala.collection.mutable.ListBuffer


/**
 * Created by ruixiang on 5/20/15.
 */
object LocalLDA {
  var dic = new Dictionary()
  val K=4//topic number
  val wordtopicsInServer=scala.collection.mutable.HashMap[Int,Array[Int]]()//now at the first time to push to server
  val topicsInServer=Array[Int](K)//now at the first time to push to server

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("LDA_Local")

    val spark = new SparkContext(conf)
    Thread.sleep(3000)

//    val trainingFile: String = "../LDA4j/data/mini2/IT_100.txt"
    val trainingFile: String = "newdocs2.dat"
    val rawLines = spark.textFile(trainingFile)


//    val words = rawLines.flatMap(line => line.split(" ")).map((_,1))
      val words = rawLines.flatMap(line => line.split(" ")).collect()
      words.foreach(x=>dic.addWord(x))
//    val vocabulary = words.reduceByKey(_+_).collect()
//    vocabulary.foreach(x=>dic.addWord(x._1))




    println("the training file's lines number:"+rawLines.count())

    var rddTopic = rawLines.mapPartitions(transFromString2LDAData)
//    rddTopic = rddTopic.repartition(9)
    println("rddTopic partition: " + rddTopic.partitions.length)

    val config: TrainingContext = new TrainingContext();
    config.iteration(5);
    config.miniBatchSize(1);
    config.psCount(1);
//    config.workerCount(1)//it does not work now,dependent on partition number

    val m: Model = new LDAModel(0.5f, 0.1f,K,dic.getSize)

    TrainingHelper.startTraining(spark, m, rddTopic, config,new LDAModelWriter(dic));
//    TrainingHelper.startTraining(spark, m, rddTopic, config,new LDAModelWriter());
    System.out.println("LDA has ended!")
  }

  def transFromString2LDAData(itr:Iterator[String])={
    var lst=ListBuffer[LDADataMatrix]();

    while(itr.hasNext) {
      val line=itr.next();
      val words=line.split(" ")

      var wordIDs=new ListBuffer[Int]();
      words.foreach(x=>wordIDs+=dic.getID(x))

      val topics=new ListBuffer[Int]()
      wordIDs.foreach(x=>topics+=Math.floor(Math.random()*K).toInt)
//      wordIDs.foreach(x=>topics+=4)


      val doctopic=new Array[Int](K)
      topics.foreach(x=>doctopic(x)=doctopic(x)+1)

      lst += new LDADataMatrix(topics.toArray,wordIDs.toArray,doctopic)

//      wordIDs.foreach(x=>{
//        if(wordtopicsInServer.contains(x)){
//
//        }
//        else{
//          wordtopicsInServer+=(x->Array[Int](K))
//          val tmp= wordtopicsInServer.get(x)
//          tmp
//        }
//
//      })
    }
    lst.iterator
  }

}
