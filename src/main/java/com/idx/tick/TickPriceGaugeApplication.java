package com.idx.tick;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TickPriceGaugeApplication {

	public static void main(String[] args) {
		SpringApplication.run(TickPriceGaugeApplication.class, args);
	}

}
