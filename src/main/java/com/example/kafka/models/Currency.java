package com.example.kafka.models;

import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum Currency {
	
	AED("AED"),
	AUD("AUD"),
	BRL("BRL"),
	CAD("CAD"),
	CHF("CHF"),
	CLP("CLP"),
	CNY("CNY"),
	COP("COP"),
	CZK("CZK"),
	DKK("DKK"),
	EUR("EUR"),
	GBP("GBP"),
	HKD("HKD"),
	HUF("HUF"),
	IDR("IDR"),
	ILS("ILS"),
	INR("INR"),
	JPY("JPY"),
	KRW("KRW"),
	MXN("MXN"),
	MYR("MYR"),
	NOK("NOK"),
	NZD("NZD"),
	PHP("PHP"),
	PLN("PLN"),
	RON("RON"),
	RUB("RUB"),
	SAR("SAR"),
	SEK("SEK"),
	SGD("SGD"),
	THB("THB"),
	TRY("TRY"),
	TWD("TWD"),
	ZAR("ZAR");
	
	private String code;
	
	Currency(String code) {
		this.code = code;
	}
	
	@JsonCreator
	public static Currency decode(String code) {
		return Stream.of(Currency.values())
				.filter(c -> c.code.equals(code))
				.findFirst()
				.orElse(null);
	}
	
	public String getCode() {
		return code;
	}
	
}
