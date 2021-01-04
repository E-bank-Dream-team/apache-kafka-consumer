package com.example.kafka.models;

import java.util.Date;

import org.springframework.format.annotation.DateTimeFormat;

public class Transaction {
	
	private Long id;
	
	private String iban;
	
	@DateTimeFormat(pattern = "yyyy-MM-dd")
	private Date date;
	
	private Double amount;
	
	private Currency currency;
	
	private String description;
	
	public Long getId() {
		return id;
	}
	
	public void setId(Long id) {
		this.id = id;
	}
	
	public String getIban() {
		return iban;
	}
	
	public void setIban(String iban) {
		this.iban = iban;
	}
	
	public Date getDate() {
		return date;
	}
	
	public void setDate(Date date) {
		this.date = date;
	}
	
	public Double getAmount() {
		return amount;
	}
	
	public void setAmount(Double amount) {
		this.amount = amount;
	}
	
	public Currency getCurrency() {
		return currency;
	}
	
	public void setCurrency(Currency currency) {
		this.currency = currency;
	}
	
	public String getDescription() {
		return description;
	}
	
	public void setDescription(String description) {
		this.description = description;
	}
	
}
