package com.example.priority;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableKafka
@EnableScheduling
@SpringBootApplication
public class PriorityApplication {
    public static void main(String[] args) {
        SpringApplication.run(PriorityApplication.class, args);
    }
}
