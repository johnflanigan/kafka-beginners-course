package com.github.johnflanigan.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);
	private final List<String> terms = Lists.newArrayList("kafka");

	private TwitterProducer() {

	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	private void run() {
		LOGGER.info("Setup");
		// Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

		// Create a twitter client
		Client client = createTwitterClient(msgQueue);

		// Attempts to establish a connection.
		client.connect();

		// Create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();

		// Add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			LOGGER.info("Stopping application");
			LOGGER.info("Shutting down client from twitter");
			client.stop();
			LOGGER.info("Closing producer");
			producer.close();
			LOGGER.info("Stopped application");
		}));

		// Loop to send tweets to kafka
		while (!client.isDone()) {
			try {
				String msg = msgQueue.poll(5, TimeUnit.SECONDS);
				if (msg != null) {
					LOGGER.info(msg);
					producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, e) -> {
						if (e != null) {
							LOGGER.error("Something bad happened", e);
						}
					});
				}
			}
			catch (InterruptedException e) {
				LOGGER.error("Client poll interrupted", e);
				client.stop();
			}
		}

		LOGGER.info("End of application");
	}


	private Authentication authenticate() {
		String consumerKey = "";
		String consumerSecret = "";
		String token = "";
		String secret = "";

		try (InputStream input = new FileInputStream("src/main/resources/keys.properties")) {
			Properties properties = new Properties();
			properties.load(input);

			consumerKey = properties.getProperty("api-key");
			consumerSecret = properties.getProperty("api-secret-key");
			token = properties.getProperty("access-token");
			secret = properties.getProperty("access-token-secret");
		}
		catch (IOException ex) {
			LOGGER.error("Error loading keys", ex);
		}

		return new OAuth1(consumerKey, consumerSecret, token, secret);
	}

	private BasicClient createTwitterClient(BlockingQueue<String> msgQueue) {
		// Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		hosebirdEndpoint.trackTerms(this.terms);

		Authentication hosebirdAuth = authenticate();

		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")                              // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		return builder.build();
	}

	private KafkaProducer<String, String> createKafkaProducer() {
		String bootstrapServers = "127.0.0.1:9092";

		// Create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create the producer
		return new KafkaProducer<>(properties);
	}
}
