package jp.itohiro.spark.playground;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkLicenseWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkLicenseWordCount Application");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> license = sc.textFile("hdfs://localhost:9000/spark/input/LICENSE.txt");
        JavaRDD<String> words = license.flatMap(s -> Arrays.asList(s.split(" ")));
        JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        counts.saveAsTextFile("hdfs://localhost:9000/spark/output/spark_license_wordcount.txt");
    }
}
