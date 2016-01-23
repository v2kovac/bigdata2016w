package ca.uwaterloo.cs.bigdata2016w.v2kovac.assignment2;

import collection.mutable.HashMap
import scala.collection.JavaConverters._
import java.util.StringTokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import org.apache.spark.util.{CollectionsUtils, Utils}

trait Tokenizer {
  def tokenize(s: String): List[String] = {
    new StringTokenizer(s).asScala.toList
      .map(_.asInstanceOf[String].toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""))
      .filter(_.length != 0)
  }
}

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer  {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}

class HashPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case (k1,k2) => (k1.hashCode & Int.MaxValue) % numPartitions
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    var marginal = 0.0

    val textFile = sc.textFile(args.input())
    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val bigrams = tokens.sliding(2).map(p => (p.head,p.tail.mkString)).toList
          val individuals = tokens.init.map(w => (w,"*")).toList
          bigrams ++ individuals
        } else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .repartitionAndSortWithinPartitions(new HashPartitioner(args.reducers()))
      .map(p => p._1 match {
        case (_,"*") => {marginal = p._2; (p._1,p._2)}
        case (_,_) => (p._1,p._2 / marginal)
      })
      .saveAsTextFile(args.output())
  }
}