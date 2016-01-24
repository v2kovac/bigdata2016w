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
import scala.collection.mutable.Map

class Conf2(args: Seq[String]) extends ScallopConf(args) with Tokenizer  {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Stripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    var marginal = 0.0

    val textFile = sc.textFile(args.input())
    textFile
      .flatMap(line => {
        val m = Map[String,Map[String,Int]]()
        val tokens = tokenize(line)
        val bigrams = tokens.sliding(2).toList.foreach(p => {
          if (m contains p.head) {
              val pMap = m.get(p.head).get
              val pTail = p.tail.mkString
              if (pMap contains pTail) {
                  pMap += (pTail -> (pMap.get(pTail).get + 1))
              } else {
                  pMap += (pTail -> 1)
              }
          } else {
              val pMap = Map[String,Int]()
              pMap += (p.tail.mkString -> 1)
              m += (p.head -> pMap)
          }
        })
        m.keys.foldLeft(List[(String,Map[String,Int])]())((l,k) => (k,m.get(k).get) :: l)
      })
      .reduceByKey((map1, map2) => {
        map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0)) }
      })
      /*.map(p => p._1 match {
        case (_,"*") => {marginal = p._2; (p._1,p._2)}
        case (_,_) => (p._1,p._2 / marginal)
      })*/
      .saveAsTextFile(args.output())
  }
}