package com.example.priority.service;

import com.example.priority.model.Message;
import com.example.priority.util.PoissonLoadGenerator;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Random;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProducerService {
    @Value(value = "${priority-enabled}")
    private Boolean priorityEnabled;
    @Value(value = "${spring.kafka.high-topic}")
    private String highTopic;
    @Value(value = "${spring.kafka.bulk-topic}")
    private String bulkTopic;
    @Value(value = "${poisson.duration-minutes}")
    private int DURATION_MINUTES;
    @Value(value = "${poisson.high-priority-chance}")
    private Double HIGH_CHANCE;
    @Value(value = "${poisson.lambda}")
    private Double LAMBDA;  //messages per second
    private final KafkaTemplate<String, Message> kafkaTemplate;
    private final Random random = new Random();
    private final TaskScheduler scheduler;
    private PoissonLoadGenerator poissonGenerator;
    private final ConfigurableApplicationContext ctx;


    @SneakyThrows
    @EventListener(ApplicationReadyEvent.class)
    public void poissonPublish() {
        Thread.sleep(10_000); // wait for app started
        Runnable task = () -> {
            var message = new Message(System.currentTimeMillis(), isHighPriority(), "payload-" + random.nextDouble());
            if (priorityEnabled) {
                kafkaTemplate.send(message.highPriority() ? highTopic : bulkTopic, message);
            } else {
                kafkaTemplate.send(highTopic, message);
            }
            log.info("Sent in Kafka: {}", message);
        };

        var poissonGenerator = new PoissonLoadGenerator(scheduler, task, LAMBDA, ctx);
        this.poissonGenerator = poissonGenerator;
        poissonGenerator.start(Duration.ofMinutes(DURATION_MINUTES));
        log.info("PoissonGenerator started");
    }

    @PreDestroy
    void shutdown() {
        if (poissonGenerator != null) poissonGenerator.stop();
    }

    private boolean isHighPriority() {
        return random.nextDouble() < HIGH_CHANCE;
    }
}
