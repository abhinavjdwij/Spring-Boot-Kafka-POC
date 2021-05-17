package com.abhinavjdwij.poc.kafkaproducer.service;

import com.abhinavjdwij.poc.kafkaproducer.kafka.constants.KafkaMasterConstants;
import com.abhinavjdwij.poc.kafkaproducer.models.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProducerService {

    @Autowired
    @Qualifier("GenericKafkaTemplate")
    private KafkaTemplate kafkaTemplate;

    public void publishMessage(String message) {
        kafkaTemplate.send(KafkaMasterConstants.DISPLAY_MESSAGE_TOPIC, message);
        System.out.println("Message sent to Kafka");
    }

    public void addPerson(List<Person> persons) {
        // validations (if any)
        kafkaTemplate.send(KafkaMasterConstants.CREATE_PERSON_TOPIC, persons);
        System.out.println("Person creation request sent to Kafka");
    }

    public void deletePerson(Person person) {
        // validations (if any)
        kafkaTemplate.send(KafkaMasterConstants.DELETE_PERSON_TOPIC, person);
        System.out.println("Person deletion request sent to Kafka");
    }

}
