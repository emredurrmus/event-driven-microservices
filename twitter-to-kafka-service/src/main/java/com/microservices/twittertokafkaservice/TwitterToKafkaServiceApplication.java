package com.microservices.twittertokafkaservice;

import com.microservices.appconfigdata.config.TwitterToKafkaServiceConfigData;
import com.microservices.twittertokafkaservice.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);
	private final TwitterToKafkaServiceConfigData configData;
	private final StreamRunner streamRunner;



	public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfigData configData,
											StreamRunner runner) {
		this.configData = configData;
		this.streamRunner = runner;
	}

	public static void main(String[] args) {
		SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
	}


	@Override
	public void run(String... args) throws Exception {
		LOG.info("App starts...");
		LOG.info(Arrays.toString(configData.getTwitterKeywords().toArray(new String[] {})));
		LOG.info(configData.getWelcomeMessage());
		streamRunner.start();
	}
}
