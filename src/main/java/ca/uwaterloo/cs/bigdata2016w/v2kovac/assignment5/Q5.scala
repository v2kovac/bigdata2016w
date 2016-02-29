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
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
}

object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf5(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    val cusMap:HashMap[Int,Int] = HashMap()

    val customer = sc.textFile(args.input() + "/customer.tbl")
    customer
      .map(line => {
        val a = line.split("\\|")
        (a(0), a(3))
      })
      .collect()
      .foreach(p => {
        cusMap += (p._1.toInt -> p._2.toInt)
      })

    val bCusMap = sc.broadcast(cusMap)

    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
    val l = lineitems
      .map(line => {
        val a = line.split("\\|")
        //just output date up to month
        (a(0), a(10).substring(0,7))
      })

    val orders = sc.textFile(args.input() + "/orders.tbl")
    orders
      .map(line => {
        val a = line.split("\\|")
        (a(0), a(1).toInt)
      })
      .filter(p => {
        val nkey = bCusMap.value(p._2)
        //canada = 3, us = 24
        nkey == 3 || nkey == 24
      })
      .cogroup(l)
      .filter(p => {
        !p._2._2.isEmpty && !p._2._1.isEmpty
      })
      .flatMap(p => {
        val ckey = p._2._1.iterator.next()
        p._2._2.map(d => ((bCusMap.value(ckey), d), 1)).toList
      })
      .reduceByKey(_ + _)
      .collect()
      .foreach(p => {
        println((p._1._1, p._1._2, p._2))
      })
  }
}





