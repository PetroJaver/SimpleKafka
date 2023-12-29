package com.sample.kafka;

import org.apache.kafka.common.internals.Topic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class Producer {
    private KafkaTemplate<String, String> kafkaTemplate;
    private final String TOPIC;

    private final List<String> STOCKS = Collections.unmodifiableList(new ArrayList<>() {
        {
            add("AAPL");
            add("GD");
            add("BRK.B");
        }
    });

    private final Map<String, Double> lastPrices = new HashMap<>() {
        {
            put("AAPL", 494.64);
            put("GD", 86.74);
            put("BRK.B", 113.59);
        }
    };

    public Producer(KafkaTemplate<String, String> kafkaTemplate, @Value("${topic.name}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.TOPIC = topic;
    }

    @Scheduled(fixedRate = 4000L)
    public void sendMessage() {
        String symbol = STOCKS.get((int) (Math.random() * 3));
        double lastPrice = updatePrice(symbol);
        String message = String.format("Quote... %s is now %.2f", symbol, lastPrice);

        kafkaTemplate.send(TOPIC, message);
    }

    private double updatePrice(String symbol) {
        boolean isPriceUp = ((int) (Math.random() * 2) + 1) % 2 == 0;
        double changeValue = Math.random() * 10;
        double lastPrice = lastPrices.get(symbol);

        if (isPriceUp) {
            lastPrice += changeValue;
        } else {
            lastPrice -= changeValue;
        }

        lastPrices.put(symbol, lastPrice);

        return lastPrice;
    }
}
