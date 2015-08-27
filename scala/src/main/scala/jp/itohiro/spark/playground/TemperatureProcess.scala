package jp.itohiro.spark.playground

import java.sql.Timestamp

import org.apache.spark.{SparkContext, SparkConf}

object TemperatureProcess {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Temperature Process App")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val statsFile = sc.textFile("/home/spark/input/stats")

    val stats = statsFile.map(s => s.split(",")).map(s => Stats(s(0), s(1), Timestamp.valueOf(s(2)), Integer.parseInt(s(3))))

    val df = stats.toDF().groupBy("host").avg("temp")

    df.show()
  }

  case class Stats(host:String, ip:String, datetime:Timestamp, temp: Integer)
}
