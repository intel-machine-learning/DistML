package com.intel.distml.example.feature

import org.apache.spark._
import org.apache.spark.mllib.feature.Word2Vec

/**
 * Created by yunlong on 1/29/16.
 */
object MllibWord2Vec {

  def main(args: Array[String]) {

    val p = Integer.parseInt(args(1))
    println("partitions: " + p)

    val conf = new SparkConf().setAppName("Word2Vec")
    val sc = new SparkContext(conf)

    val input = sc.textFile(args(0)).map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()
    word2vec.setNumPartitions(p)

    val model = word2vec.fit(input)
    println("vocab size: " + model.getVectors.size)
    val synonyms = model.findSynonyms("black", 40)

    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    sc.stop()

  }
}
