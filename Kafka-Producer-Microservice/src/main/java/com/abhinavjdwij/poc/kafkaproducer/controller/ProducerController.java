package com.abhinavjdwij.poc.kafkaproducer.controller;

import com.abhinavjdwij.poc.kafkaproducer.models.Person;
import com.abhinavjdwij.poc.kafkaproducer.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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
    public String addPerson(@RequestBody Person person) {
        service.addPerson(person);
        return "Add Person Request Sent";
    }

    @PostMapping(value = "/deletePerson", consumes = "application/json")
    public String deletePerson(@RequestBody Person person) {
        service.deletePerson(person);
        return "Delete Person Request Sent";
    }
}
