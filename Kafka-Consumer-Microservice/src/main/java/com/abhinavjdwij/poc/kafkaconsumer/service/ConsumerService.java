package com.abhinavjdwij.poc.kafkaconsumer.service;

import com.abhinavjdwij.poc.kafkaconsumer.kafka.constants.KafkaMasterConstants;
import com.abhinavjdwij.poc.kafkaconsumer.kafka.utility.KafkaMasterUtility;
import com.abhinavjdwij.poc.kafkaconsumer.models.Person;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ConsumerService {

    @KafkaListener(
            topics = KafkaMasterConstants.DISPLAY_MESSAGE_TOPIC,
            groupId = "${spring.kafka.group.id}",
            containerFactory = "GenericConcurrentListener"
    )
    public void sendMessageSubscriber(String message) {
        System.out.println("Message received from Kafka " + message);
    }

    @KafkaListener(
            topics = KafkaMasterConstants.CREATE_PERSON_TOPIC,
            groupId = "${spring.kafka.group.id}",
            containerFactory = "GenericConcurrentListener"
    )
    public void createPersonSubscriber(ConsumerRecord consumerRecord) {
        System.out.println("Message received from Kafka to create persons");
        List<Person> persons = KafkaMasterUtility.consumerRecordValueParser(consumerRecord, new TypeReference<List<Person>>(){});
        System.out.println("Person list size = " + persons.size());
        persons.forEach(System.out::println);
    }

}
