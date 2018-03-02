package com.dxc.be.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class SparkContext {
    private static Logger LOGGER = LoggerFactory.getLogger(SparkContext.class);

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("Hello Spark");
        sparkConf.setMaster("local");

        JavaSparkContext context = new JavaSparkContext(sparkConf);

        JavaRDD<String> textFile = context.textFile("src/main/resources/test.txt");
        /**JavaPairRDD<String, Integer> counts =*/ textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .map(s -> s.replaceAll(
                        "[,.!?:;]", "")           // Remove punctuation and transfer to lowercase
                        .trim()
                        .toLowerCase())
                .zipWithIndex()
                .mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2().toString()))
                .reduceByKey((a, b) -> a + "," + b)
                .sortByKey()
                .collect()
                .forEach(x -> LOGGER.info(x.toString()));

        context.close();

    }

}
