package com.example.kafka.streams;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import com.example.kafka.ApacheKafkaConsumerApplication;
import com.example.kafka.models.Transaction;
import com.example.kafka.models.TransactionRequest;
import com.example.kafka.utils.KafkaFeeder;

@Component
public class KafkaTopologyFactory {
	
	private final static Logger logger = LoggerFactory.getLogger(ApacheKafkaConsumerApplication.class);
	
	private static String inputTopic;
	private static String outputTopic;
	private static String bootstrapAddress;
	
	public static Topology getTransactionsTopology() {
		StreamsBuilder builder = new StreamsBuilder();
		
		Consumed<String, TransactionRequest> consumed = Consumed.with(Serdes.String(), new JsonSerde<TransactionRequest>(TransactionRequest.class));
		Produced<String, List<Transaction>> produced = Produced.with(Serdes.String(), new JsonSerde<List<Transaction>>(List.class));
		
		KStream<String, TransactionRequest> inputStream = builder.stream(inputTopic, consumed);
		KStream<String, List<Transaction>> outputStream = inputStream.mapValues((k, v) -> {
			logger.info(String.format("Processing: [%s]", v));
			return KafkaFeeder.getMockedTransactions();
		});
		
		outputStream.to(outputTopic, produced);
		
		return builder.build();
	}
	
	public static Properties getConfig() {
		Properties config = new Properties();
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "other-group");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()
				.getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
		config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.kafka.models");
		return config;
	}
	
	@Value("${default.kafka.input.topic.name}")
	public void setInputTopicName(String name) {
		KafkaTopologyFactory.inputTopic = name;
	}
	
	@Value("${default.kafka.output.topic.name}")
	public void setOutputTopicName(String name) {
		KafkaTopologyFactory.outputTopic = name;
	}
	
	@Value("${spring.kafka.bootstrap-servers}")
	public void setBootstrapServers(String name) {
		KafkaTopologyFactory.bootstrapAddress = name;
	}
	
}
