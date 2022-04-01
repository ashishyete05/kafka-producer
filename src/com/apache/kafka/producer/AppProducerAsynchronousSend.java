package com.apache.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AppProducerAsynchronousSend {

	public static void main(String[] args) {

		String topicName = "my_message";
		String key = "Key-1";
		String value = "Asynch Prouducer Message : -1";

		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(prop);
		ProducerRecord<String, String> producerRec = new ProducerRecord<>(topicName, key, value);

		producer.send(producerRec, new MyProducerCallback());
		producer.close();
		System.out.println("Kafka message Sent");

	}

}

class MyProducerCallback implements Callback {

	public void onCompletion(RecordMetadata rm, Exception e) {
		if (e != null) {
			System.out.println("Asynch Message Sending Failed");
		} else {
			System.out.println("Asynch Message Ack Success to Partition : "+rm.partition() +" and offset : "+rm.offset());
		}

	}
}
