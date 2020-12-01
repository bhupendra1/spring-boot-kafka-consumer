package com.bhupendra.practice.springbootkafkaconsumer.service;

import com.bhupendra.practice.springbootkafkaconsumer.model.Customer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerListener {
    Logger LOG = LoggerFactory.getLogger(KafkaConsumerListener.class);

    @KafkaListener(topics = "customer", groupId = "group_id")
    public void consumeMessages(String message) throws JsonProcessingException {
        Customer customer = new ObjectMapper().readValue(message, Customer.class);
        LOG.info("Message received: {}", customer);
    }
}
