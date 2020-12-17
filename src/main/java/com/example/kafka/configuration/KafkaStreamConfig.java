package com.example.kafka.configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.example.kafka.models.Transaction;
import com.example.kafka.models.TransactionRequest;
import com.example.kafka.utils.KafkaFeeder;

@Configuration
@EnableKafkaStreams
public class KafkaStreamConfig {
	
	private final static Logger logger = LoggerFactory.getLogger(KafkaStreamConfig.class);
	
	@Value("${default.kafka.input.topic.name}")
	private String inputTopic;
	
	@Value("${default.kafka.output.topic.name}")
	private String outputTopic;
	
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapAddress;
	
	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration kStreamsConfigs(KafkaProperties kafkaProperties) {
		
		Map<String, Object> config = new HashMap<>();
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "other-group");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()
				.getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
		config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.kafka.models");
		return new KafkaStreamsConfiguration(config);
	}
	
	@Bean
    public KStream<String, List<Transaction>> kStream(StreamsBuilder kStreamBuilder) {
        
        Consumed<String, TransactionRequest> consumed = Consumed.with(Serdes.String(), new JsonSerde<TransactionRequest>(TransactionRequest.class));
        Produced<String, List<Transaction>> produced = Produced.with(Serdes.String(), new JsonSerde<List<Transaction>>(List.class));
        
        KStream<String, TransactionRequest> inputStream = kStreamBuilder.stream(inputTopic, consumed);
        KStream<String, List<Transaction>> outputStream = inputStream.mapValues((k, v) -> {
            logger.info(String.format("Processing: [%s]", v));
            return KafkaFeeder.getMockedTransactions();
        });
        
        outputStream.to(outputTopic, produced);
        return outputStream;
    }
	
}
