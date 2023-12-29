package com.sample.kafka;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.logging.Logger;

@Configuration
public class Consumer {
    private final Logger logger = Logger.getLogger(this.getClass().getName());
    @KafkaListener(topics = "${topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void listener(String message) {
        logger.info("Received" + message);
    }
}
