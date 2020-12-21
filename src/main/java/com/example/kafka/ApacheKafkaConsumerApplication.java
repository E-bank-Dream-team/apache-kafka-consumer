package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.example.kafka.streams.KafkaStreamRunner;

@SpringBootApplication
public class ApacheKafkaConsumerApplication {
	
	public static void main(String[] args) throws InterruptedException {
		ConfigurableApplicationContext context = SpringApplication.run(ApacheKafkaConsumerApplication.class, args);
		
		KafkaStreamRunner runner = context.getBean(KafkaStreamRunner.class);
		runner.run();
	}
}
