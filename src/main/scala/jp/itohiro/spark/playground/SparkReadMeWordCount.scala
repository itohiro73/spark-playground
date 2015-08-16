package jp.itohiro.spark.playground

import org.apache.spark.{SparkContext, SparkConf}

object SparkReadMeWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkLicenseWordCount Application")
    val sc = new SparkContext(conf)
    val readMe = sc.textFile("hdfs://localhost:9000/spark/input/README.txt")

    val counts = readMe.flatMap(line => line.split(" "))
          .map(word => (word, 1))
          .reduceByKey((a, b) => a + b)

    counts.saveAsTextFile("hdfs://localhost:9000/spark/output/spark_readme_wordcount.txt")
  }
}
