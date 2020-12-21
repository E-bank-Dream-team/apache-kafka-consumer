package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.example.kafka.streams.KafkaStreamRunner;

@SpringBootApplication
public class ApacheKafkaConsumerApplication {
	
	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(ApacheKafkaConsumerApplication.class, args);
		
		KafkaStreamRunner streamsRunner = new KafkaStreamRunner();
		streamsRunner.run();
	}	
}
