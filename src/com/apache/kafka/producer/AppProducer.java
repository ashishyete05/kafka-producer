package com.apache.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AppProducer {

	public static void main(String[] args) throws InterruptedException {

		String topicName = "my_message";
		String key = "Key-1";
		String value = "Ayanaa-";

		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(prop);
		ProducerRecord<String, String> producerRec =null;
		
		for(int i=0;i<100;i++) {
			 producerRec = new ProducerRecord<>(topicName, key, value+i);
			producer.send(producerRec);
			System.out.println("Message Sent");
			Thread.sleep(2000);
		}
		
		//producer.send(producerRec);
		producer.close();
		System.out.println("Kafka message Sent");

	}

}
