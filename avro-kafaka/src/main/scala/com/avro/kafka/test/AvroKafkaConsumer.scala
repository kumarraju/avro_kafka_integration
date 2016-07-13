package com.avro.kafka.test

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.avro.mapred.AvroKey
import org.apache.spark.sql.SQLContext
//import org.apache.avro.mapred.AvroValue
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import org.apache.avro.generic.GenericRecord
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.storage.StorageLevel._

import io.confluent.kafka.serializers.KafkaAvroDecoder
//import org.apache.hadoop.io.serializer.avro.AvroRecord
//import org.apache.spark.streaming.dstream.ForEachDStream

import org.apache.spark.sql.SQLContext
import org.apache.kafka.common.serialization.Deserializer

object AvroKafkaConsumer {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: AvroKafkaConsumer <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }
    val Array(brokers, topics) = args
    val WINDOW_LENGTH = Minutes(10)//set according to requirement-- include data for last 10 minutes.
    val SLIDE_INTERVAL = Seconds(60)// at every 60s, a new RDD is created.
    val sparkConf = new SparkConf().setAppName("AvroKafkaConsumer").setMaster("local[1]")
    sparkConf.registerKryoClasses(Array(classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]]))
    val ssc = new StreamingContext(sparkConf, SLIDE_INTERVAL)
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> "consumer",
      "zookeeper.connect" -> "localhost:2181", "schema.registry.url" -> "http://localhost:8081") //, 
    /*val messages = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, topicsSet)
    messages.print()*/

    val messages = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2.toString).persist(MEMORY_AND_DISK) // choose persistent level 
    val windowLineDstream = lines.window(WINDOW_LENGTH, SLIDE_INTERVAL)
    windowLineDstream.foreachRDD(jsonRDD => {
      val sqlContext = SQLContextSingleton.getInstance(jsonRDD.sparkContext)
      val data = sqlContext.read.json(jsonRDD)
      data.printSchema()
      data.registerTempTable("clicklog")
      println("ten most searched destinations")
      val result = sqlContext.sql("select destination, count(*) as total_click from clicklog where action='search' group by destination order by total_click desc limit 10")
      result.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

object SQLContextSingleton {
  @transient private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}