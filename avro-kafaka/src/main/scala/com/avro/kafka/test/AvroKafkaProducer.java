package com.avro.kafka.test;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class AvroKafkaProducer {
	private static KafkaProducer<Object, Object> producer;
	private static String brokers;
	private static String topic;
	private static String fileName;
	static String schemaStr = "{\n" + "	\"namespace\": \"avro.trivago\",\n"
			+ "	\"type\": \"record\",\n" + "	\"name\": \"ClickLog\",\n"
			+ "	\"fields\": [\n"
			+ "		{\"name\": \"user_id\", \"type\": \"long\"},\n"
			+ "		{\"name\": \"time\", \"type\": \"string\"},\n"
			+ "		{\"name\": \"action\", \"type\": \"string\"},\n"
			+ "		{\"name\": \"destination\", \"type\": \"string\"},\n"
			+ "		{\"name\": \"hotel\", \"type\": \"string\"}\n" + "	]\n" + "}";

	public static void main(String[] args) throws FileNotFoundException,
			NumberFormatException, InterruptedException {
		if (args.length < 3) {
			System.err
					.println("Usage: AvroKafkaProducerNew <brokers> <topic> <dataFile>");
			System.exit(1);
		}
		brokers = args[0];
		topic = args[1];
		fileName = args[2];
		System.out.println("Avro producer Started...");
		AvroKafkaProducer avroProducer = new AvroKafkaProducer();
		avroProducer.initProducer();
		// while(true){// reading file in indefinite loop
		File text = new File(fileName);
		Scanner scnr = new Scanner(text);
		String[] row;
		String line;
		while (scnr.hasNextLine()) {
			line = scnr.nextLine();
			row = line.split(",");
			System.out.println(line);
			// System.out.println(row.length);
			if (row.length == 5)
				avroProducer.produce(Long.parseLong(row[0]), row[1], row[2],row[3], row[4]);
		}

	}

	private void initProducer() {
		System.out.println("Initializing Kafka producer.");
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put("schema.registry.url", "http://localhost:8081");
		producer = new KafkaProducer(props);
	}

	public void produce(long user_id, String time, String action,
			String destination, String hotel) throws InterruptedException {
		String key = "K" + Math.random() + user_id;
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(schemaStr);
		GenericRecord avroRecord = new GenericData.Record(schema);
		avroRecord.put("user_id", user_id);
		avroRecord.put("time", time);
		avroRecord.put("action", action);
		avroRecord.put("destination", destination);
		avroRecord.put("hotel", hotel);

		ProducerRecord record = new ProducerRecord<Object, Object>(topic, key,avroRecord);
		try {
			producer.send(record);
		} catch (SerializationException e) {
			// may need to do something with it
			e.printStackTrace();
		}
		TimeUnit.MILLISECONDS.sleep(500);
	}
}
