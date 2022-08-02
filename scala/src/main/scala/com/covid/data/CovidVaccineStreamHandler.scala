package com.covid.data

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import java.sql.Timestamp

case class CovidVaccinationData(event_time: Timestamp, centre_name: String, vaccination_completed: Int, male: Int, female: Int)

object CovidVaccineStreamHandler {
  def main(args: Array[String]) {

    // initialize Spark
    val spark = SparkSession
      .builder
      .appName("Covid Vaccine Stream Handler")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
      .getOrCreate()

    import spark.implicits._

    // read from Kafka
    val inputDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "covid-vaccine-data")
      .load()

    val rawDf = inputDF.selectExpr("CAST(value AS STRING)").as[String]

    val expandedDf = rawDf.map(row => row.split(","))
      .map(row => CovidVaccinationData(
        Timestamp.valueOf(row(0)),
        row(1),
        row(2).toInt,
        row(3).toInt,
        row(4).toInt))


    // groupby and aggregate
    //    val summaryDf = expandedDf
    //      .groupBy("centre_name").agg(avg("vaccination_completed"))

    val windowedAvgSignalDF = expandedDf
      .groupBy(col("centre_name"), window(col("event_time"), "1 minute", "40 seconds"))
      .agg(avg("vaccination_completed"))

    //TODO : write to file with partitioning with event time
    //TODO : write to console with other job
    val query = windowedAvgSignalDF
      .writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .option("truncate", "false")
      .format("console")
      .start()
    query.awaitTermination()
  }
}