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

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
}

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)

    val date = args.date()

    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
    val l = lineitems
      .filter(line => {
        line.split("\\|")(10) contains date
      })
      .map(line => {
        (line.split("\\|")(0).toInt, 0)
      })

    val orders = sc.textFile(args.input() + "/orders.tbl")
    orders
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, a(6))
      })
      .cogroup(l)
      .filter(p => {
        !p._2._2.isEmpty
      })
      .sortByKey()
      .take(20)
      .foreach(p => {
        println((p._2._1.iterator.next(),p._1))
      })
  }
}





