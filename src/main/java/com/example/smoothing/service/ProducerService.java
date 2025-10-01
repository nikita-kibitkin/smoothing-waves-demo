package com.example.smoothing.service;

import com.example.smoothing.generator.SquareWaveRate;
import com.example.smoothing.generator.StochasticLoadGenerator;
import com.example.smoothing.generator.TimedTask;
import com.example.smoothing.generator.batchsize.GeometricBatchSize;
import com.example.smoothing.model.Message;
import com.example.smoothing.smoothing.BackpressureGate;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
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
    @Value(value = "${spring.kafka.topic}")
    private String kafkaTopic;
    @Value(value = "${load-generator.duration-minutes}")
    private int DURATION_MINUTES;
    @Value(value = "${load-generator.high-rate}")
    private int HIGH_RATE;
    @Value(value = "${backpressure.enabled}")
    private Boolean backpressureEnabled;
    private final KafkaTemplate<String, Message> kafkaTemplate;
    private final Random random = new Random();
    private final TaskScheduler scheduler;
    private final BackpressureGate backpressureGate;
    private final ConfigurableApplicationContext ctx;
    @Getter
    private StochasticLoadGenerator stochasticGenerator;


    @SneakyThrows
    @EventListener(ApplicationReadyEvent.class)
    public void stochasticPublish() {
        Thread.sleep(10_000); // wait Kafka

        TimedTask kafkaSendTask = (t0) -> {
            var message = new Message(t0, "payload-" + random.nextDouble() + random.nextDouble() + random.nextDouble());
            kafkaTemplate.send(kafkaTopic, message);
            //log.info("Sent in Kafka: {}", message);
        };

        StochasticLoadGenerator slg = StochasticLoadGenerator.builder()
                .scheduler(scheduler)                        // твой TaskScheduler
                .task(kafkaSendTask)                                  // твоя нагрузка
                .rate(SquareWaveRate
                        .of(2.0, HIGH_RATE, Duration.ofMinutes(1), 0.25)  // low=2 rps, high=50 rps, период=1м, duty=25%
                        .withJitter(0.1))                           // ±10% рваность краёв
                .batchSampler(GeometricBatchSize.ofMean(5)) // средняя пачка ~5
                .intraBatchSpread(Duration.ofMillis(200))   // разнести k задач по ~200мс
                .ctx(ctx)
                .backpressureGate(backpressureEnabled ? backpressureGate : null)// оставить, как в твоём stop()
                .build();
        this.stochasticGenerator = slg;

        slg.start(Duration.ofMinutes(DURATION_MINUTES));
        log.info("StochasticGenerator started");
    }

    @PreDestroy
    void shutdown() {
        if (stochasticGenerator != null) stochasticGenerator.stop();
    }

}
