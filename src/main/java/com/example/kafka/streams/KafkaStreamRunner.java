package com.example.kafka.streams;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamRunner {
	
	private final KafkaTopologyFactory topologyFactory;
	
	public KafkaStreamRunner(KafkaTopologyFactory topologyFactory) {
		this.topologyFactory = topologyFactory;
	}
	
	private final static Logger logger = LoggerFactory.getLogger(KafkaStreamRunner.class);
	
	public void run() {
		logger.info("Running streams...");
		Topology topology = topologyFactory.getTransactionsTopology();
		Properties config = topologyFactory.getConfig();
		@SuppressWarnings("resource")
		KafkaStreams ks = new KafkaStreams(topology, config);
		ks.start();
	}
	
}
