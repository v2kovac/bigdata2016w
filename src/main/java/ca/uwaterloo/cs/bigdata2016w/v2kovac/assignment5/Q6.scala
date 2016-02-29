package ca.uwaterloo.cs.bigdata2016w.v2kovac.assignment5;

import collection.mutable.HashMap
import scala.collection.JavaConverters._
import java.util.StringTokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.util.{CollectionsUtils, Utils}

class Conf5(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
}

object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf5(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
    lineitems
      .filter(line => {
        line.split("\\|")(10) contains date
      })
      .map(line => {
        val a = line.split("\\|")
        val l_q = a(4).toDouble
        val l_ep = a(5).toDouble
        val l_d = a(6).toDouble
        val l_t = a(7).toDouble
        ((a(8), a(9)), (l_q, l_ep, l_ep*(1-l_d), l_ep*(1-l_d)*(1+l_t), 1))
      })
      .reduceByKey(((a,b,c,d,e),(a2,b2,c2,d2,e2)) => {
        (a+a2, b+b2, c+c2, d+d2, e+e2)
      })
      .foreach(p => println(p))
  }
}





