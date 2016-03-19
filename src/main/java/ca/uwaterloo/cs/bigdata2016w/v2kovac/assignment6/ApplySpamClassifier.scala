package ca.uwaterloo.cs.bigdata2016w.v2kovac.assignment6;

import collection.mutable.HashMap
import scala.collection.JavaConverters._
import java.util.StringTokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math.exp

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, output)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val output = opt[String](descr = "output path", required = true)
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())
  val w = HashMap[Int, Double]()

  // Scores a document based on its list of features.
  def spamminess(features: Array[Int]) : Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("Apply Spam Classifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //load model
    sc.textFile(args.model())
      .map(line => {
        val a = line.split(",")
        (a(0).substring(1), a(1).substring(0, a(1).length-1))
      })
      .collect()
      .foreach(p => {
        w(p._1.toInt) = p._2.toDouble
      })

    //classify text data
    val textFile = sc.textFile(args.input())
    textFile
      .map(line => {
        val tokens = line.split(" ")
        val features = tokens.slice(2,tokens.length).map(_.toInt)
        val score = spamminess(features)
        val isSpam = if (score > 0) "spam" else "ham"
        (tokens(0), tokens(1), score, isSpam)
      })
      .saveAsTextFile(args.output())

  }
}
