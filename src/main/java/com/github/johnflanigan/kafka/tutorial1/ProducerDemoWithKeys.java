package com.github.johnflanigan.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

	public static void main(String[] args) throws ExecutionException, InterruptedException {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

		String bootstrapServers = "127.0.0.1:9092";

		// create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		for (int i = 0; i < 10; i++) {
			// create a producer record
			String topic = "first_topic";
			String value = String.format("hello world %d", i);
			String key = String.format("id_%d", i);

			ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

			logger.info("Key: {}", key);
			// id_0 is going to partition 1
			// id_1 partition 0
			// id_2 partition 2
			// id_3 partition 0
			// id_4 partition 2
			// id_5 partition 2
			// id_6 partition 0
			// id_7 partition 2
			// id_8 partition 1
			// id_9 partition 2

			// send data - asynchronous
			producer.send(record, (recordMetadata, e) -> {
				// executes every time a record is successfully sent or an exception is thrown
				if (e == null) {
					// the record was successfully sent
					logger.info("Received new metadata.\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
							recordMetadata.topic(),
							recordMetadata.partition(),
							recordMetadata.offset(),
							recordMetadata.timestamp());
				}
				else {
					logger.error("Error while producing", e);
				}
			}).get(); // block the .send() to make it synchronous - don't do this in production!
		}

		// flush data
		producer.flush();
		// flush and close producer
		producer.close();
	}
}
