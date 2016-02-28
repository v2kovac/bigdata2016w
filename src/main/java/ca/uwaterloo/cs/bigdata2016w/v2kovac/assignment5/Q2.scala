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

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
}

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val date = args.date()

    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
    lineitems
      .filter(line => {
        line.split("\\|")(10) contains date
      })
      .map(line => {
        (line.split("\\|")(0), 0)
      })

    val orders = sc.textFile(args.intpu() + "/orders.tbl")
    orders
      .map(line => {
        val a = line.split("\\|")
        (a(0), a(6))
      })
      .cogroup(lineitems)
      .foreach(x => println(x))
  }
}





