package com.github.access.engine;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * Processes access logs and derives statistics like
 * 1. visited URLs with count
 * 2. ips with security breach history (tried to access /admin url more than 5 times)
 * 3. Average response time
 */
public class AccessLogProcessor {
    public static void main(String[] args) {

        // Run in local dev
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("AccessLogProcessor");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Cache for further use
        JavaRDD<String> lines = sparkContext.textFile("data/access_log").cache();

        // Print URL & number of times it is visited
        List<Tuple2<String, Integer>> collect = lines.map(line -> StringUtils.substring(line, line.indexOf("\""), line.lastIndexOf("\"")).trim())
                .map(line -> StringUtils.substring(line, line.indexOf(" "), line.lastIndexOf(" ")).trim())
                .mapToPair(url -> Tuple2.apply(url, 1))
                .reduceByKey((a, b) -> a + b)
                .collect();
        System.out.println(collect);

        // ips with security breach history (tried to access /admin url more than 5 times)
        Map<String, Integer> ipBlackListMap = lines.filter(line -> StringUtils.contains(line, "/admin"))
                .filter(line -> StringUtils.contains(line, "403"))
                .map(line -> StringUtils.substring(line, 0, line.indexOf(" ")))
                .mapToPair(ip -> Tuple2.apply(ip, 1))
                .reduceByKey((a, b) -> a + b)
                .filter(tuple -> tuple._2() >= 5)
                .collectAsMap();
        System.out.println(ipBlackListMap);

        double avgResponseTime = lines.map(line -> StringUtils.substring(line, line.lastIndexOf(' '), line.length()))
                .mapToDouble(NumberUtils::toDouble)
                .mean();
        System.out.println("Average response time for User - " + avgResponseTime);

        sparkContext.stop();
    }
}
