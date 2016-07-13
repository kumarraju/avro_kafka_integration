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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.kafka.common.serialization.Deserializer

import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext

object AvroKafkaConsumer2 {
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
    val WINDOW_LENGTH = Minutes(60*24)//set according to requirement-- include data for last 24 hours.
    val SLIDE_INTERVAL = Seconds(60)// at every 60s, a new RDD is created.--set according to requirement.
    val sparkConf = new SparkConf().setAppName("AvroKafkaConsumer").setMaster("local[1]").set("spark.driver.allowMultipleContexts", "true")
    // val sc = new SparkContext(sparkConf)
    // val hiveContext = new HiveContext(sc)
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
      val hiveContext = HiveContextSingleton.getInstance(jsonRDD.sparkContext)
      val data = hiveContext.read.json(jsonRDD)
      data.printSchema()
      data.registerTempTable("clicklog")

      //data.select(data("user_id") , data("time"), row_number().over(Window.partitionBy("user_id").orderBy(data("time").desc)).alias("group_rank")).show()
      //data.groupBy('word).count

      println("SECOND_DISTINCT_HOTEL_CLICK_ON_LAST_DAY for each user_id")
      val hotel_click_query = """Select GET_LAST_USER.user_id, SECOND_DISTINCT_HOTEL_CLICK.hotel as SECOND_DISTINCT_HOTEL_CLICK_ON_LAST_DAY From
                                      (Select user_id, group_rank
                                      From
                                      (
                                      	Select user_id, time,
                                      		row_number() over (partition by user_id order by time desc) as group_rank
                                      	From clicklog where action='click'
                                      ) t1 
                                      where group_rank=1)  GET_LAST_USER -- get_last_day_user -- sort data in descending order and get 1st row to get last day user data
                                      
                                      
                                      LEFT OUTER JOIN 
                                      
                                      (Select user_id, hotel
                                      	From
                                      	(
                                      	Select user_id, hotel, time, row_number() over (partition by user_id order by time desc) as group_rank 
                                      				From
                                      					(Select user_id, hotel, time
                                      						From
                                      							(	Select user_id, hotel, time,
                                      											row_number() over (partition by user_id, hotel order by time desc) as group_rank
                                      								From clicklog where action='click'
                                      							) t2 
                                      							Where  group_rank = 1
                                      					 ) DISTINCT_HOTEL_CLICK -- unique record of (user_id, hotel)
                                      	) t3 Where  group_rank = 2 -- second hotel click
                                      ) SECOND_DISTINCT_HOTEL_CLICK
                                      ON (GET_LAST_USER.user_id = SECOND_DISTINCT_HOTEL_CLICK.user_id)"""
      println(hotel_click_query)

      //val hotel_click_query = "Select user_id, hotel, time, row_number() over (partition by user_id, hotel order by time desc) as group_rank From clicklog where action='click'"
      val result = hiveContext.sql(hotel_click_query)
      result.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

object HiveContextSingleton {
  @transient private var instance: HiveContext = _
  def getInstance(sparkContext: SparkContext): HiveContext = {
    if (instance == null) {
      instance = new HiveContext(sparkContext)
    }
    instance
  }
}