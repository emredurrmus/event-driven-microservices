package com.microservices.twittertokafkaservice.init.impl;

import com.microservices.appconfigdata.config.KafkaConfigData;
import com.microservices.kafka.admin.config.client.KafkaAdminClient;
import com.microservices.twittertokafkaservice.init.StreamInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamInitializer implements StreamInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamInitializer.class);

    private final KafkaConfigData kafkaConfigData;

    private final KafkaAdminClient kafkaAdminClient;

    public KafkaStreamInitializer(KafkaConfigData kafkaConfigData, KafkaAdminClient kafkaAdminClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaAdminClient = kafkaAdminClient;
    }

    @Override
    public void init() {
        kafkaAdminClient.createTopics();
        kafkaAdminClient.checkShemaRegistry();
        LOG.info("Topics with name {} created", kafkaConfigData.getTopicNamesToCreate().toArray());
    }
}
