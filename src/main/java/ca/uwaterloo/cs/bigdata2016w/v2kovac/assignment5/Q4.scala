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

class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
}

object Q4 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val date = args.date()
    val cusMap:HashMap[Int,Int] = HashMap()
    val natMap:HashMap[Int,String] = HashMap()

    val customer = sc.textFile(args.input() + "/customer.tbl")
    customer
      .map(line => {
        val a = line.split("\\|")
        (a(0), a(3))
      })
      .collect()
      .foreach(p => {
        partMap += (p._1.toInt -> p._2.toInt)
      })

    val nation = sc.textFile(args.input() + "/nation.tbl")
    nation
      .map(line => {
        val a = line.split("\\|")
        (a(0), a(1))
      })
      .collect()
      .foreach(p => {
        suppMap += (p._1.toInt -> p._2)
      })

    val bCusMap = sc.broadcast(cusMap)
    val bNatMap = sc.broadcast(natMap)

    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
    val l = lineitems
      .filter(line => {
        line.split("\\|")(10) contains date
      })
      .map(line => {
        (line.split("\\|")(0), 0)
      })

    val orders = sc.textFile(args.input() + "/orders.tbl")
    orders
      .map(line => {
        val a = line.split("\\|")
        (a(0), a(1).toInt)
      })
      .cogroup(l)
      .filter(p => {
        !p._2._2.isEmpty
      })
      .map(p => {
        val nkey = bCusMap.value(p._2._1.iterator.next())
        (nkey, 1) 
      })
      .reduceByKey(_ + _)
      .foreach(p => {
        println((p._1, bNatMap(p._1), p._2))
      })

  }
}





