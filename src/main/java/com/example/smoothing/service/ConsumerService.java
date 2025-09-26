package com.example.smoothing.service;

import com.example.smoothing.metrics.LatencyMetrics;
import com.example.smoothing.metrics.ThroughputMetrics;
import com.example.smoothing.model.Message;
import com.example.smoothing.smoothing.BackpressureGate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConsumerService {
    private final BackpressureGate backpressureGate;
    @Value(value = "${backpressure-active}")
    private Boolean backpressureActive;

    @KafkaListener(topics = {"${spring.kafka.topic}"}, concurrency = "8")
    public void handle(Message msg) {
        try {
            emulateWorkAndRecordMetrics(msg);
        } catch (Exception e) {
            log.error("Error in emulateWorkAndRecordMetrics", e);
        } finally {
            grantBackpressureCredit();
        }
    }

    private void emulateWorkAndRecordMetrics(Message msg) throws InterruptedException {
        Thread.sleep(30);
        var latency = System.currentTimeMillis() - msg.startTimeMs();
        LatencyMetrics.getHistogram().recordValue(latency);
        ThroughputMetrics.incrementThroughputCount();
        log.info("Latency recorded: latency={} ms", latency);
    }

    private void grantBackpressureCredit(){
        if(backpressureActive){
            backpressureGate.grant(1);
        }
    }

}