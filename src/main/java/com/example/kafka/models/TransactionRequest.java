package com.example.kafka.models;

import java.time.LocalDate;

public class TransactionRequest {
	
	private Long customerId;
	private Long accountId;
	private LocalDate date;
	
	public TransactionRequest(Long customerId, Long accountId, LocalDate date) {
		this.customerId = customerId;
		this.accountId = accountId;
		this.date = date;
	}
	
	@SuppressWarnings("unused")
	private TransactionRequest() {
		super();
	}
	
	public Long getCustomerId() {
		return customerId;
	}
	
	public void setCustomerId(Long customerId) {
		this.customerId = customerId;
	}
	
	public Long getAccountId() {
		return accountId;
	}
	
	public void setAccountId(Long accountId) {
		this.accountId = accountId;
	}
	
	public LocalDate getDate() {
		return date;
	}
	
	public void setDate(LocalDate date) {
		this.date = date;
	}
	
}
