package com.microservices.kafka.producer.config.service.impl;

import com.microservices.kafka.avro.model.TwitterAvroModel;
import com.microservices.kafka.producer.config.service.KafkaProducer;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaProducer.class);

    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PreDestroy
    public void close() {
        if (kafkaTemplate != null) {
            LOG.info("Closing producer");
            kafkaTemplate.destroy();
        }
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel value) {
        LOG.info("Sending record {} to topic {}", value, topicName);
        CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResult = kafkaTemplate.send(topicName, key, value);
        kafkaResult.whenComplete((result, exception) -> {
            addCallBack(topicName, value, result, exception);
        });
    }

    private void addCallBack(String topicName, TwitterAvroModel value, SendResult<Long, TwitterAvroModel> result, Throwable exception) {
        if (exception != null) {
            LOG.error("Error while sending message {} to topic {}", value, topicName, exception);
        } else {
            RecordMetadata recordMetadata = result.getRecordMetadata();
            LOG.info("Received new metadata. Topic: {} Partition: {} Offset: {} Timestamp: {}",
                    recordMetadata.topic(),
                    recordMetadata.partition(),
                    recordMetadata.offset(),
                    recordMetadata.timestamp());
        }
    }
}
