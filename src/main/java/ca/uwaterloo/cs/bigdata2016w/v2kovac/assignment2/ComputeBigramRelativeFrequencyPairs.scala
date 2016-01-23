package ca.uwaterloo.cs.bigdata2016w.v2kovac.assignment2;

import collection.mutable.HashMap
import java.util.StringTokenizer
import scala.collection.JavaConverters._

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}

object WordCount extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def tokenize(s: String): List[String] = {
    new StringTokenizer(s).asScala.toList
      .map(_.asInstanceOf[String].toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""))
      .filter(_.length != 0)
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(" ")).toList else List()
      })
      /*.map(word => (word, 1))
      .reduceByKey(_ + _)*/
      .saveAsTextFile(args.output())
  }
}