package com.example.kafka.consumer;

import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.example.kafka.models.Transaction;

@Service
public class TransactionConsumer {
	
	@KafkaListener(topics = "${default.kafka.output.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
	public void consume(List<Transaction> message) {
		System.out.println("message: " + message);
		// BFLogger.logInfo(String.format("Consumed message: %s", message));
	}
	
}
