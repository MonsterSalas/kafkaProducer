package com.example.kafka_producer.service;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class KafkaProducerService {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    private static final String TOPIC = "seniales-vital";
    private final Random random = new Random();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    @PostConstruct
    public void init() {
        startScheduledMessageSending();
    }
    
    public void startScheduledMessageSending() {
        scheduler.scheduleAtFixedRate(
            this::sendRandomVitalSign,
            0,
            10,
            TimeUnit.SECONDS
        );
    }
    private void sendRandomVitalSign() {
        String message = random.nextBoolean() ? 
            "signos vitales normales" : 
            "signos vitales alterados";   
        kafkaTemplate.send(TOPIC, message);
    }
    public void sendMessage(String message) {
        kafkaTemplate.send(TOPIC, message);
    }
    
    @PreDestroy
    public void cleanup() {
        scheduler.shutdown();
    }
}