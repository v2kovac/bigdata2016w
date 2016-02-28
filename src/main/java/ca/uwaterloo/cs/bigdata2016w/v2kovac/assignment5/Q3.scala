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

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
}

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val date = args.date()
    val partMap:HashMap[Int,String] = HashMap()
    val suppMap:HashMap[Int,String] = HashMap()

    val part = sc.textFile(args.input() + "/part.tbl")
    part
      .collect()
      .foreach(line => {
        val a = line.split("\\|")
        partMap += (a(0).toInt -> a(1))
      })

    val supplier = sc.textFile(args.input() + "/supplier.tbl")
    supplier
      .collect()
      .foreach(line => {
        val a = line.split("\\|")
        suppMap += (a(0).toInt -> a(1))
      })

    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
    lineitems
      .filter(line => {
        line.split("\\|")(10) contains date
      })
      .map(line => {
        val a = line.split("\\|")
        (a(0), (partMap(a(1).toInt), suppMap(a(2).toInt)))
      })
      .sortByKey()
      .take(20)
      .foreach(p => {
        println((p._1,p._2._1,p._2._2))
      })
  }
}





