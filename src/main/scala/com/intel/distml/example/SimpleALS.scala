package com.intel.distml.example

import akka.japi.Util
import breeze.numerics.{sqrt, abs}
import com.intel.distml.api.{Session, Model}
import com.intel.distml.platform.DistML
import com.intel.distml.util.scala.DoubleMatrixWithIntKey
import com.intel.distml.util.{KeyCollection, KeyList, DoubleMatrix}
import org.apache.commons.math3.linear._

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, BitSet}
import scala.util.{Random, Sorting}
import collection.JavaConverters._


import org.jblas.{DoubleMatrix => DM, Solve, SimpleBlas}
/**
  * Created by jimmy on 16-3-15.
  */

object SimpleALSObject{
  private case class Params (
                            master: String = null,
                            psNum:  Int = 1,
                            ratingFile: String = null,
                            rank: Int = 10,
                            iterations: Int = 10,
                            outputdir: String = null,
                            lambda: Double = 0.1,
                            implicitPrefs: Boolean = false,
                            alpha: Double = 1,
                            blocks: Int = 1
                            )

  def main(args: Array[String]): Unit = {
    val alsParams = Params()
    val parser = new OptionParser[Params]("ALSExample") {
      head("ALSExample: an example ALS app for plain text data.")
      opt[String]("master")
        .text(s"master url for spark master: ${alsParams.master}")
        .action((x, c) => c.copy(master = x))
      opt[Int]("psNum")
        .text(s"ps server number for ALS : ${alsParams.psNum}")
        .action((x, c) => c.copy(psNum = x))
      opt[Int]("rank")
        .text(s"features number for ALS : ${alsParams.rank}")
        .action((x, c) => c.copy(rank = x))
      opt[String]("ratingFile")
        .text(s"rating file for training: ${alsParams.ratingFile}")
        .action((x, c) => c.copy(ratingFile = x))
      opt[Int]("iterations")
        .text(s"number of iterations of training. default: ${alsParams.iterations}")
        .action((x, c) => c.copy(iterations = x))
      opt[String]("outputdir")
        .text(s"output paths (directories) default: ${alsParams.outputdir}")
        .action((x, c) => c.copy(outputdir = x))
      opt[Double]("lambda")
        .text(s" lambda for als default: ${alsParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Double]("alpha")
        .text(s"alpha for the als default: ${alsParams.alpha}")
        .action((x, c) => c.copy(alpha = x))
      opt[Int]("blocks")
        .text(s"blocks for als user/product split default ${alsParams.blocks}")
        .action((x, c) => c.copy(blocks = x))
      opt[Boolean]("implicitPrefs")
        .text(s"if implicitPrefs is enable default: ${alsParams.implicitPrefs}")
        .unbounded()
        .required()
        .action((x, c) => c.copy(implicitPrefs = x))
    }

    parser.parse(args, alsParams).map {
      params => run(params)
    }.getOrElse {
      parser.showUsageAsError
      System.exit(1)
    }
  }

case class InLinkBlock (
                         elementIds: Array[Int],
                         ratingsForBlock: Array[Array[(Array[Int], Array[Double])]]
                       )

case class OutLinkBlock(
                         elementIds: Array[Int],
                         shouldSend: Array[BitSet]
                       )

case class Rating (var uid: Int, var pid: Int, var rating: Double)


def run(params: Params) {

    val conf = new SparkConf().setAppName("SimpleALS")
    val sc = new SparkContext(conf)

    val ratings = sc.textFile(params.ratingFile).map {line =>
      val fields = line.split(',')
      Rating(fields(0).toInt - 1, fields(1).toInt - 1, fields(2).toDouble)
    }
    val U = ratings.map(_.uid).distinct().count()
    //val V = ratings.map(_.pid).distinct().count()
    //workround for gaps exsits in userids in prize
    val V = ratings.map(_.pid).distinct().max() + 1
    println("xxxxxxxxxxxxxxxxxxxxxxxxxxxx V" + V)

    val numBlocks = params.blocks
    val rank = params.rank
    val iterations = params.iterations

    val model = new Model{
      registerMatrix("user", new DoubleMatrixWithIntKey(U, rank))
      registerMatrix("product", new DoubleMatrixWithIntKey(V, rank))
    }

    val dm = DistML.distribute(ratings.context, model, params.psNum)
    val partitioner = new HashPartitioner(numBlocks)

    val ratingsByUserBlock = ratings.map {
      rating => (rating.uid % numBlocks, rating)
    }

    val ratingsByProductBlock = ratings.map { rating =>
      (rating.pid % numBlocks, Rating (rating.pid, rating.uid, rating.rating))
    }

    val (userInLinks, userOutLinks) = makeLinkRDDs (numBlocks, ratingsByUserBlock)
    val (productInLinks, productOutLinks)  = makeLinkRDDs(numBlocks, ratingsByProductBlock)

    dm.random("user")
    dm.random("product")

    val mPath = dm.monitorPath

    //store to parameter server

    for (iter <- 1 to iterations) {
      //update product
      updateFeatures(userOutLinks, productInLinks, partitioner, rank, params.lambda, params.alpha, true, dm)

      updateFeatures(productOutLinks, userInLinks, partitioner,rank, params.lambda, params.alpha, false, dm)
      if (iter % 5 == 0){
        //compute rsme
        println("iter :  " + iter + "  RMSE == " + computeRMSE(ratings, dm))
      }
    }

    //put final model to parameter server
    //todo
   // sc.stop()
    dm.recycle()
    sc.stop()
  }

  def computeRMSE(ratings: RDD[Rating], dm: DistML[Int]) : Double = {
    val session = new Session(dm.model, dm.monitorPath,1)
    val usersM = dm.model.getMatrix("user").asInstanceOf[DoubleMatrixWithIntKey]
    val productsM = dm.model.getMatrix("product").asInstanceOf[DoubleMatrixWithIntKey]
    val users = usersM.fetch(KeyCollection.ALL, session)
    val products = productsM.fetch(KeyCollection.ALL, session)
    var sumErr = 0.0
    def dot(l: Array[Double], r: Array[Double]): Double = {
      var sum = 0.0
      for(i <- 0 until l.length) {
        sum += l(i) * r(i)
      }
      sum
    }

    sumErr = ratings.map{ rating =>
      val diff = rating.rating - dot(users(rating.uid), products(rating.pid))
      //println(diff)
      diff * diff
    }.reduce(_+_)
   math.sqrt(sumErr / ((users.size).toDouble * (products.size).toDouble))
   // math.sqrt(sumErr)
  }

  def updateFeatures(
                    //products: RDD[(Int, Array[Array[Double]])],
                    productOutLinks: RDD[(Int, OutLinkBlock)],
                    userInLinkBlocks: RDD[(Int, InLinkBlock)],
                    partitioner: Partitioner,
                    rank: Int,
                    lambda: Double,
                    alpha: Double,
                    flag: Boolean, // true user, false product,
                    dm: DistML[Int],
                    implicitPrefs: Boolean = false
                    //YtY: Broadcast[Any]
                    ) {
    val numBlocks = productOutLinks.partitions.size
    val model = dm.model
    val monitorPath = dm.monitorPath
    productOutLinks.flatMap { case (bid, outLinkBlock) =>
      val session = new Session(model, monitorPath, bid)
      val matrix = if (flag) model.getMatrix("user").asInstanceOf[DoubleMatrixWithIntKey]
                    else model.getMatrix("product").asInstanceOf[DoubleMatrixWithIntKey]

      val toSend = Array.fill(numBlocks)(new ArrayBuffer[Array[Double]])
      val keys = new KeyList()
      for(p <- 0 until outLinkBlock.elementIds.length; userBlock <- 0 until numBlocks) {
        /*
        if(outLinkBlock.shouldSend(p)(userBlock)) {
          val keys = new KeyList()
          keys.addKey(outLinkBlock.elementIds(p))
          val result_tmp = matrix.fetch(keys, session)
          val result: Array[Double] = result_tmp(outLinkBlock.elementIds(p))
          toSend(userBlock) += result
        }
        */
        if(outLinkBlock.shouldSend(p)(userBlock)) {
          keys.addKey(outLinkBlock.elementIds(p))
        }

      }

      val result_tmp = matrix.fetch(keys, session)
      for(p <- 0 until outLinkBlock.elementIds.length; userBlock <- 0 until numBlocks) {
        if(outLinkBlock.shouldSend(p)(userBlock)) {
          toSend(userBlock) += result_tmp(outLinkBlock.elementIds(p))
        }
      }
      toSend.zipWithIndex.map { case (buf, idx) => (idx, (bid, buf.toArray))}
    }.groupByKey(partitioner)
    .join(userInLinkBlocks)
    .mapValues { case (messages, inLinkBlock) =>
        val factors_new = updateBlock(messages.toSeq, inLinkBlock, rank, lambda, alpha)
        val session = new Session(model, monitorPath, 1)
        val matrix = if (flag) model.getMatrix("product").asInstanceOf[DoubleMatrixWithIntKey]
                      else model.getMatrix("user").asInstanceOf[DoubleMatrixWithIntKey]
        val keys = new KeyList()
        for (p <- 0 until inLinkBlock.elementIds.length) {
          keys.addKey(inLinkBlock.elementIds(p))
        }
        val factors_old = matrix.fetch(keys, session)
        for (p <- 0 until inLinkBlock.elementIds.length) {
          val key = inLinkBlock.elementIds(p)
          val grid = new Array[Double](rank)
          for (i <-0 until rank ) {
            grid(i) = factors_new(p)(i) - factors_old(key)(i)
          }
          factors_old.put(key, grid)
        }
        matrix.push(factors_old, session)
    }.count()
  }


  def updateBlock(
                   messages: Seq[(Int, Array[Array[Double]])], inLinkBlock: InLinkBlock,
                   rank: Int, lambda: Double, alpha: Double, implicitPrefs: Boolean = false
                   //, YtY: Broadcast[Any]
                 ): Array[Array[Double]] = {
    val blockFactors = messages.sortBy(_._1).map(_._2).toArray
    val numBlocks = blockFactors.length
    val numUsers = inLinkBlock.elementIds.length

    val triangleSize = rank * (rank + 1) /2
    val userXtX = Array.fill(numUsers)(DM.zeros(triangleSize))
    val userXy = Array.fill(numUsers)(DM.zeros(rank))

    val tempXtX = DM.zeros(triangleSize)
    val fullXtx = DM.zeros(rank, rank)

    for(productBlock <- 0 until numBlocks) {
      for (p <-0 until blockFactors(productBlock).length) {
        val x = new DM(blockFactors(productBlock)(p))
        //x.data.foreach((arg: Double) => //println(arg))
        //println("xxxxxxxxxxxxxxxxxxxxx factor befor xxxxxxxxxxxx")
        tempXtX.fill(0.0)
        dspr(1.0, x, tempXtX)
        //println("xxxxxxxxxxxxxxxxxxxxxxxxxx after dspr xxxxxxxxxxxxxx")
       // tempXtX.data.foreach((arg: Double ) => //println(arg))
        val (us, rs) = inLinkBlock.ratingsForBlock(productBlock)(p)
        //println("xxxxxxxxxxxxxxxxxxxx rating info   xxxxxx")
        //us.foreach( x => //println(x))

       // rs.foreach(x => //println(x))
        for (i <- 0 until us.length) {
          implicitPrefs match {
            case false =>
              userXtX(us(i)).addi(tempXtX)
              SimpleBlas.axpy(rs(i), x, userXy(us(i)))
              //println("after axpy ")
             // userXy(us(i)).data.foreach(x=> //println("xxxxxxxxxxxxxxxxxxxxxxx " + x.toString ))
            case true =>
              None
          }
        }
      }
    }
    Array.range(0, numUsers).map { index =>
     // userXtX(index).data.foreach( (arg: Double) => //println(arg))
      fillFullMatrix(userXtX(index), fullXtx)
      (0 until rank).foreach(i => fullXtx.data(i*rank + i) += lambda)
      implicitPrefs match {
        case false => {
          //println("data 10 * 10")
          //fullXtx.data.foreach((arg: Double) => //println(arg))
          //println("data 10 * 1")
          //userXy(index).data.foreach( (arg: Double) => //println(arg))
          Solve.solvePositive(fullXtx, userXy(index)).data
          //val xtx = new Array2DRowRealMatrix(rank, rank)
         // val xty = new ArrayRealVector(rank)
          //xtx.getData = fullXtx.data
         // new CholeskyDecomposition(xtx).getSolver.solve(xty).toArray
          //new CholeskyDecomposition(fullXtx).getSolver.solve(userXy(index))
        }
        //case true => None
      }
    }

  }

  def fillFullMatrix(triangleMatrix: DM, destMatrix: DM): Unit = {
    val rank = destMatrix.rows
    var i = 0
    var pos = 0
    while (i < rank) {
      var j = 0
      while (j <= i) {
        destMatrix.data(i*rank + j) = triangleMatrix.data(pos)
        destMatrix.data(j*rank + i) = triangleMatrix.data(pos)

        pos += 1
        j += 1
      }
      i += 1
    }

  }

  def dspr( alpha: Double, x: DM, L: DM) = {
    val n = x.length
    var i = 0
    var j = 0
    var idx = 0

    var axi = 0.0
    val xd = x.data
    val Ld = L.data

    while(i<n) {
      axi = alpha * xd(i)
      j = 0
      while (j <= i) {
        Ld(idx) += axi * xd(j)
        j+=1
        idx += 1
      }
      i += 1
    }
  }

  def computeYtY(factors: RDD[(Int, Array[Array[Double]])]) = {
    val implicitPrefs = false
    if(implicitPrefs) {

    } else {
      None
    }
  }

  def makeOutLinkBlock(numBlocks: Int, ratings: Array[Rating]): OutLinkBlock = {
    val userIds = ratings.map(_.uid).distinct.sorted
    val numUsers = userIds.length
    val userIdToPos = userIds.zipWithIndex.toMap
    val shouldSend = Array.fill(numUsers)(new BitSet(numBlocks))

    for (r <- ratings) {
      shouldSend(userIdToPos(r.uid))(r.pid%numBlocks) = true
    }
    OutLinkBlock(userIds, shouldSend)
  }

  def makeInLinkBlock(numBlocks: Int, ratings: Array[Rating]): InLinkBlock = {
    val userIds = ratings.map(_.uid).distinct.sorted
    val numUsers = userIds.length
    val userIdToPos = userIds.zipWithIndex.toMap
    val blockRatings = Array.fill(numBlocks)(new ArrayBuffer[Rating])
    for (r <- ratings) {
      blockRatings(r.pid % numBlocks) += r
    }
    val ratingsForBlock = new Array[Array[(Array[Int], Array[Double])]](numBlocks)
    for (productBlock <- 0 until numBlocks) {
      val groupRatings = blockRatings(productBlock).groupBy(_.pid).toArray
      val ordering = new Ordering[(Int, ArrayBuffer[Rating])] {
        def compare(a: (Int, ArrayBuffer[Rating]), b: (Int, ArrayBuffer[Rating])): Int = a._1 - b._1

      }
      Sorting.quickSort(groupRatings)(ordering)
      ratingsForBlock(productBlock) = groupRatings.map { case (p, rs) =>
        (rs.view.map(r => userIdToPos(r.uid)).toArray, rs.view.map(_.rating).toArray)
      }

    }
    InLinkBlock(userIds, ratingsForBlock)
  }

  def makeLinkRDDs(numBlocks: Int,ratings: RDD[(Int, Rating)])
  : (RDD[(Int, InLinkBlock)], RDD[(Int, OutLinkBlock)]) = {

    val links = ratings.groupByKey(new HashPartitioner(numBlocks)).map { case (blockid, iter) => {
      //makeInLinkBlock
      val userIds = new ArrayBuffer[Int]()
      val blockRatings = Array.fill(numBlocks)(new ArrayBuffer[Rating])
      for (it <- iter) {
        userIds += it.uid
        blockRatings(it.pid % numBlocks) += it
      }

      val uIds = userIds.distinct.sorted
      val numUsers = uIds.length
      val uIdToPos = uIds.zipWithIndex.toMap
      val ratingsForBlock = new Array[Array[(Array[Int], Array[Double])]](numBlocks)
      for (productBlock <- 0 until numBlocks) {
        val groupRatings = blockRatings(productBlock).groupBy(_.pid).toArray

        val ordering = new Ordering[(Int, ArrayBuffer[Rating])] {
          def compare(a: (Int, ArrayBuffer[Rating]), b: (Int, ArrayBuffer[Rating])): Int = a._1 - b._1

        }
        Sorting.quickSort(groupRatings)(ordering)
        ratingsForBlock(productBlock) = groupRatings.map { case (p, rs) =>
          (rs.view.map(r => uIdToPos(r.uid)).toArray, rs.view.map(_.rating).toArray)
        }
      }
      val inlinkBlcok = InLinkBlock(uIds.toArray, ratingsForBlock)

      //makeOutLinkBlock
      val shouldSend = Array.fill(numUsers)(new BitSet(numBlocks))
      for (it <- iter) {
        shouldSend(uIdToPos(it.uid))(it.pid % numBlocks) = true
      }
      val outLinkBlock = OutLinkBlock(uIds.toArray, shouldSend)
      (blockid, (inlinkBlcok, outLinkBlock))
    }

    }

    links.persist(StorageLevel.MEMORY_AND_DISK)
    (links.mapValues(_._1), links.mapValues(_._2))
  }

    //links.persist(StorageLevel.MEMORY_AND_DISK)
    //(inblocks, outblocks)
      /*


    val grouped = ratings.partitionBy(new HashPartitioner(numBlocks))
    val links = grouped.mapPartitionsWithIndex((blockId, elements) => {
      val ratings = elements.map{_._2}.toArray
      val inLinkBlock = makeInLinkBlock(numBlocks, ratings)
      val outLinkBlock = makeOutLinkBlock(numBlocks, ratings)
      Iterator.single((blockId, (inLinkBlock, outLinkBlock)))
    }, true)
    links.persist(StorageLevel.MEMORY_AND_DISK)
    (links.mapValues(_._1), links.mapValues(_._2))
    */


  def randomFactor(rank: Int, rand: Random): Array[Double] = {
    val factor = Array.fill(rank)(abs(rand.nextGaussian()))
    val norm = sqrt(factor.map(x => x * x).sum)
    factor.map(x => x/norm)
  }
}
