package com.covid.handler

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark RDD demo")
      .config("spark.cassandra.connection.host", "localhost")
      //      .config("spark.default.parallelism",4)
       .config("spark.sql.shuffle.partitions",4)
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("/home/priyanka.naik/Quovantis_Work_Space/spark-structure-streaming-kafka/scala/src/resources/demoSpark.txt")
    println("rdd  partitions: ", rdd.getNumPartitions)
    rdd.foreachPartition(p => println("Items in Partitions " + p.foreach(i => println(i))))


    val rdd1 = spark.sparkContext.parallelize(Array("etc", "fonts"))
    println("rdd 1 partitions: ", rdd1.getNumPartitions)
    rdd1.foreachPartition(p => println("Items in Partitions " + p.foreach(i => println(i))))

    val rdd_in = rdd.flatMap(line => line.split("/")).map(word => (word, 1)).reduceByKey(_ + _)
    val rdd1_in = rdd1.map(word => (word, 1)).reduceByKey(_ + _)
    val count = rdd_in.intersection(rdd1_in).collect

    count.foreach(u => println(u._1, u._2))
    Thread.sleep(1000000)
  }

}
