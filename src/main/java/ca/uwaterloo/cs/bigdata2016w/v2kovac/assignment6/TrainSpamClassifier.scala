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

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val shuffle = opt[Boolean](descr = "shuffle", required = false)
}

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())
  val w = HashMap[Int, Double]()

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
    log.info("Shuffle: " + args.shuffle())

    val conf = new SparkConf().setAppName("Train Spam Classifier")
    val sc = new SparkContext(conf)

    val modelDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(modelDir, true)
    val modelOutput = args.model()

    val delta = 0.002

    var textFile = sc.textFile(args.input())

    //Shuffle
    if (args.shuffle() == true) {
      textFile = textFile
        .map(line => {
          val r = scala.util.Random
          (r.nextInt, line)
        })
        .sortByKey()
        .map(p => {
          p._2
        })
    }

    textFile
      .map(line => {
        val tokens = line.split(" ")
        val isSpam = if (tokens(1) == "spam") 1 else 0
        (0, (tokens(0), isSpam, tokens.slice(2,tokens.length).map(_.toInt)))
      })
      .coalesce(1)
      .groupByKey(1)
      .flatMap(p => {
        p._2.foreach(p2 => {
          val score = spamminess(p2._3)
          val prob = 1.0 / (1 + exp(-score))
          p2._3.foreach(f => {
            if (w.contains(f)) {
              w(f) += (p2._2 - prob) * delta
            } else {
              w(f) = (p2._2 - prob) * delta
            }
          })
        })
        w
      })
      .saveAsTextFile(modelOutput)

  }
}
