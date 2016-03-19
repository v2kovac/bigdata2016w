package ca.uwaterloo.cs.bigdata2016w.v2kovac.assignment2;

import collection.mutable.HashMap
import scala.collection.JavaConverters._
import java.util.StringTokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
}

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())
  val w = Map[Int, Double]()

  // Scores a document based on its list of features.
  def spamminess(features: Array[Int]) : Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("Train Spam Classifier")
    val sc = new SparkContext(conf)

    val modelDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(modelDir, true)

    val textFile = sc.textFile(args.input())
    textFile
      .map(line => {
        val tokens = line.split(" ")
        val isSpam = if (tokens(1) == "spam") 1 else 0
        (0, (tokens(0), isSpam, tokens.slice(2,tokens.length).map(_.toInt)))
      })
      .groupByKey(1)
      .saveAsTextFile(args.model())
  }
}
