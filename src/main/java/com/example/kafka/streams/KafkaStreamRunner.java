package com.example.kafka.streams;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamRunner {
	
	private final static Logger logger = LoggerFactory.getLogger(KafkaStreamRunner.class);
	
	public void run() {
		logger.info("Running streams...");
		Topology topology = KafkaTopologyFactory.getTransactionsTopology();
		Properties config = KafkaTopologyFactory.getConfig();
		@SuppressWarnings("resource")
		KafkaStreams ks = new KafkaStreams(topology, config);
		ks.start();
	}
	
}
