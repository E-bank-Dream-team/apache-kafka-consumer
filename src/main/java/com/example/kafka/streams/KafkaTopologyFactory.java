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

import com.example.kafka.models.Transaction;
import com.example.kafka.models.TransactionRequest;
import com.example.kafka.utils.KafkaFeeder;

@Component
public class KafkaTopologyFactory {
	
	private final static Logger logger = LoggerFactory.getLogger(KafkaTopologyFactory.class);
	
	@Value("${default.kafka.input.topic.name}")
	private String inputTopicName;
	
	@Value("${default.kafka.output.topic.name}")
	private String outputTopicName;
	
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapAddressName;
	
	public Topology getTransactionsTopology() {
		StreamsBuilder builder = new StreamsBuilder();
		
		Consumed<String, TransactionRequest> consumed = Consumed.with(Serdes.String(), new JsonSerde<TransactionRequest>(TransactionRequest.class));
		Produced<String, List<Transaction>> produced = Produced.with(Serdes.String(), new JsonSerde<List<Transaction>>(List.class));
		
		KStream<String, TransactionRequest> inputStream = builder.stream(inputTopicName, consumed);
		KStream<String, List<Transaction>> outputStream = inputStream.mapValues((k, v) -> {
			logger.info(String.format("Processing: [%s]", v));
			return KafkaFeeder.getMockedTransactions();
		});
		
		outputStream.to(outputTopicName, produced);
		
		return builder.build();
	}
	
	public Properties getConfig() {
		Properties config = new Properties();
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddressName);
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "other-group");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()
				.getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
		config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.kafka.models");
		return config;
	}
	
}
