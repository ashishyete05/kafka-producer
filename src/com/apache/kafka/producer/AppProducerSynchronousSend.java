package com.apache.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AppProducerSynchronousSend {

	public static void main(String[] args) {
		String topicName = "my_message";
		String key = "Key-";
		String value = "Ayanaa-Value";

		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(prop);
		ProducerRecord<String, String> producerRec = new ProducerRecord<>(topicName, key, value);

		try {
			RecordMetadata recordMeta=producer.send(producerRec).get();
			System.out.println("Message Sent to partition Number : "+recordMeta.partition() + " and Offset : "+recordMeta.offset());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
		producer.close();
		}
		System.out.println("Kafka message Sent");
	}

}
