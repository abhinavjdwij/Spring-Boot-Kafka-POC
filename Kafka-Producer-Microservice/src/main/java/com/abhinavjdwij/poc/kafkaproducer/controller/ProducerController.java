package com.abhinavjdwij.poc.kafkaproducer.controller;

import com.abhinavjdwij.poc.kafkaproducer.models.Person;
import com.abhinavjdwij.poc.kafkaproducer.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

@RestController
@RequestMapping("/poc")
public class ProducerController {

    @Autowired
    private ProducerService service;

    @GetMapping("/sendMessage/{message}")
    public String sendMessage(@PathVariable String message) {
        service.publishMessage(message);
        return "Send Message Request Sent";
    }

    @PostMapping(value = "/addPerson", consumes = "application/json")
    public String addPerson(@RequestBody List<Person> persons) {
        service.addPerson(persons);
        return "Add Person Request Sent";
    }

    @PostMapping(value = "/deletePerson", consumes = "application/json")
    public String deletePerson(@RequestBody Person person) {
        service.deletePerson(person);
        return "Delete Person Request Sent";
    }

    @GetMapping(value = "/bulkAddPerson/{bulkSize}")
    public String bulkAddPerson(@PathVariable Integer bulkSize) {
        String response = "Try size <= 500, larger sizes manually blocked to avoid huge log creation";
        if (bulkSize <= 500) {
            Random random = new Random();
            List<Person> persons = LongStream.range(1, bulkSize + 1)
                    .mapToObj(id -> {
                        Person person = new Person();
                        person.setId(id);
                        person.setName(UUID.randomUUID().toString().substring(0, 6));
                        person.setAge(random.nextInt(100) + 1);
                        person.setDob(null);
                        return person;
                    })
                    .collect(Collectors.toList());
            service.addPerson(persons);
            response = "Bulk Person Creation request sent with bulkSize " + bulkSize;
        }
        System.out.println(response);
        return response;
    }
}
