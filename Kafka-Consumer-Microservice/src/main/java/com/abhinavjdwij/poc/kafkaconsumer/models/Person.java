package com.abhinavjdwij.poc.kafkaconsumer.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person implements Serializable {
    private Long id;
    private String name;
    private Integer age;
    private Date dob;
}