package com.example.kafka;

import java.util.List;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.example.kafka.models.Transaction;
import com.example.kafka.models.TransactionRequest;
import com.example.kafka.utils.KafkaFeeder;

@SpringBootApplication
public class ApacheKafkaConsumerApplication {
	
	private final static Logger logger = LoggerFactory.getLogger(ApacheKafkaConsumerApplication.class);
	
	private static String inputTopic;
	private static String outputTopic;
	
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapAddress;
	
	public static void main(String[] args) {
		SpringApplication.run(ApacheKafkaConsumerApplication.class, args);
	}
	
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
	
	@Value("${default.kafka.input.topic.name}")
	public void setInputTopicName(String name) {
		ApacheKafkaConsumerApplication.inputTopic = name;
	}
	
	@Value("${default.kafka.output.topic.name}")
	public void setOutputTopicName(String name) {
		ApacheKafkaConsumerApplication.outputTopic = name;
	}
	
}
