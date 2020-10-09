package com.safaricom.analytics.streaming.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RedisKinesisProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(RedisKinesisProducerApplication.class, args);
	}

}
