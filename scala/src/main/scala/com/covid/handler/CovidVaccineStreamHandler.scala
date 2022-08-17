package com.covid.handler

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import java.io.FileInputStream
import scala.io.Source
import java.nio.file.Paths
import java.sql.Timestamp

object CovidVaccineStreamHandler {
  def main(args: Array[String]) {

    // initialize Spark
    val spark = SparkSession
      .builder
      .appName("Covid Vaccine Stream Handler")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
      .getOrCreate()

    val logMessageConfigFile = Paths.get("/home/priyanka.naik/Quovantis_Work_Space/covid-vaccine/scala/src/resources/schema.json").toString
    val fileIn: FileInputStream = new FileInputStream(logMessageConfigFile)
    val schemaSource = scala.io.Source.fromInputStream(fileIn, "utf-8").mkString
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    println(schemaFromJson.fields.mkString(","))
    import spark.implicits._

    // read from Kafka
    val inputDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "covid-vaccine-data")
      .load()

    val rawDf = inputDF
      .select(from_json(col("value").cast("string"), schemaFromJson).alias("parsed_values"))
      .select(col("parsed_values.*")).withColumn("event_date", to_date(col("event_time")))

    // groupby and aggregate
    val summaryDf = rawDf
          .groupBy("center_name").agg(avg("vaccination_completed"))
    val windowedAvgSignalDF = rawDf
      .groupBy(col("centre_name"), window(col("event_time"), "1 minute", "30 seconds"))
      .agg(avg("vaccination_completed"))

    //TODO : write to file with partitioning with event time
    //TODO : write to console with other job
    val query = summaryDf
      .writeStream
      .queryName("console_query")
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .option("truncate", "false")
      .format("console")
      .start()
    query.awaitTermination()

    val partitionBy: Seq[String] = Seq("event_date")
    val query2 = rawDf
      .writeStream
      .queryName("parquet_query")
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .format("parquet")
      .option("path", "/home/priyanka.naik/Quovantis_Work_Space/dfs/spark")
      .option("checkpointLocation", "/home/priyanka.naik/Quovantis_Work_Space/dfs/spark/checkpoint")
      .outputMode("append")
      .partitionBy(partitionBy: _*)
      .start()
    query2.awaitTermination()
  }
}