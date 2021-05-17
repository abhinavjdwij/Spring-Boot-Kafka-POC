package com.abhinavjdwij.poc.kafkaproducer.kafka.utility;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaMasterUtility {
    public static <T> T consumerRecordValueParser(ConsumerRecord consumerRecord, TypeReference<T> typeReference) {
        T serializedObject = null;
        try {
            if (consumerRecord != null && consumerRecord.value() != null) {
                ObjectMapper objectMapper = new ObjectMapper();
                serializedObject = objectMapper.convertValue(consumerRecord.value(), typeReference);
            }
        } catch (Exception e) {
            System.out.println("Consumer Record Value Parser Error");
            e.printStackTrace();
        }
        return serializedObject;
    }
}
