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
import scala.math.BigDecimal

class Conf6(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
}

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf6(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    val date = args.date()

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
        ((a(8), a(9)), (l_q, l_ep, l_ep*(1-l_d), l_ep*(1-l_d)*(1+l_t), 1, l_d))
      })
      .reduceByKey((v1, v2) => {
        (v1._1+v2._1, v1._2+v2._2, v1._3+v2._3, v1._4+v2._4, v1._5+v2._5, v1._6+v2._6)
      })
      .collect()
      .foreach(p => {
        val count = p._2._5
        val l_1 = BigDecimal(p._2._1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        val l_2 = BigDecimal(p._2._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        val l_3 = BigDecimal(p._2._3).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        val l_4 = BigDecimal(p._2._4).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        val l_5 = BigDecimal(p._2._1/count).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        val l_6 = BigDecimal(p._2._2/count).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        val l_7 = BigDecimal(p._2._6/count).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        println((p._1._1, p._1._2, l_1, l_2, l_3, l_4, l_5, l_6, l_7, count))
      })
  }
}





