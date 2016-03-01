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

class Conf7(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
}

object Q7 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf7(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val date = args.date()
    val year = date.substring(0,4).toInt
    val month = date.substring(5,7).toInt
    val day = date.substring(8,10).toInt
    val cusMap:HashMap[Int,String] = HashMap()

    val part = sc.textFile(args.input() + "/customer.tbl")
    part
      .map(line => {
        val a = line.split("\\|")
        (a(0), a(1))
      })
      .collect()
      .foreach(p => {
        cusMap += (p._1.toInt -> p._2)
      })

    val bCusMap = sc.broadcast(cusMap)

    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
    val l = lineitems
      .filter(line => {
        val l_date = line.split("\\|")(10)
        val l_year = l_date.substring(0,4).toInt
        val l_month = l_date.substring(5,7).toInt
        val l_day = l_date.substring(8,10).toInt
        (l_year > year) || (l_year == year && l_month > month) || (l_year == year && l_month == month && l_day > day)
      })
      .map(line => {
        val a = line.split("\\|")
        val l_ep = a(5).toDouble
        val l_d = a(6).toDouble
        (a(0).toInt, l_ep*(1-l_d))
      })

    val orders = sc.textFile(args.input() + "/orders.tbl")
    orders
      .filter(line => {
        val o_date = line.split("\\|")(4)
        val o_year = o_date.substring(0,4).toInt
        val o_month = o_date.substring(5,7).toInt
        val o_day = o_date.substring(8,10).toInt
        (o_year < year) || (o_year == year && o_month < month) || (o_year == year && o_month == month && o_day < day)
      })
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, (bCusMap.value(a(1).toInt), a(4), a(7)))
      })
      .cogroup(l)
      .filter(p => {
        !p._2._2.isEmpty && !p._2._1.isEmpty
      })
      .map(p => {
        val items = p._2._1.iterator.next()
        val c_name = items._1
        val l_orderkey = p._1
        val o_orderdate = items._2
        val o_shippriority = items._3
        val sum = p._2._2.foldLeft(0.0)((b,a) => b+a)
        (sum, (c_name, l_orderkey, o_orderdate, o_shippriority))
      })
      .sortByKey(false)
      .take(10)
      .foreach(p => {
        val rev = BigDecimal(p._1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        println((p._2._1, p._2._2, rev, p._2._3, p._2._4))
      })
  }
}





