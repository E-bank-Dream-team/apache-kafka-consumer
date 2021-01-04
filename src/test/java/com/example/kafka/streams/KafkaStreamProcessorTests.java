package com.example.kafka.streams;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.example.kafka.models.Transaction;
import com.example.kafka.models.TransactionRequest;

@SpringBootTest
public class KafkaStreamProcessorTests {
	
	private final static String INPUT_TOPIC = "input-topic";
	private final static String OUTPUT_TOPIC = "output-topic";
	
	private static TopologyTestDriver testDriver;
	
	@Autowired
	private KafkaTopologyFactory topologyFactory;
	
	@Test
	public void test() {
		
		Topology topology = topologyFactory.getTransactionsTopology();
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		testDriver = new TopologyTestDriver(topology, props);
		
		TestInputTopic<String, TransactionRequest> inputTopic = testDriver.createInputTopic(
				INPUT_TOPIC, new StringSerializer(), new JsonSerializer<TransactionRequest>());
		TestOutputTopic<String, List<Transaction>> outputTopic = testDriver.createOutputTopic(
				OUTPUT_TOPIC, new StringDeserializer(), new JsonDeserializer<List<Transaction>>());
		assertThat(outputTopic.isEmpty(), is(true));
		
		inputTopic.pipeInput("1", new TransactionRequest(1L, 1L, LocalDate.of(2018, 1, 20)));
		assertThat(outputTopic.isEmpty(), is(false));
		assertThat(outputTopic.readValue(), equalTo(new ArrayList<>()));
	}
	
	@AfterAll
	public static void tearDown() {
		testDriver.close();
	}
	
}
