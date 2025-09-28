package com.example.smoothing.service;

import com.example.smoothing.db.EventDao;
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
    private final EventDao eventDao;
    @Value(value = "${backpressure.enabled}")
    private Boolean backpressureEnabled;
    @Value(value = "${db-enabled}")
    private Boolean dbEnabled;

    @KafkaListener(topics = {"${spring.kafka.topic}"}, concurrency = "8")
    public void handle(Message msg) {
        try {
            if (dbEnabled) {
                saveToDBAndRecordMetrics(msg);
            }else {
                emulateWorkAndRecordMetrics(msg);
            }
        } catch (Exception e) {
            log.error("Error in emulateWorkAndRecordMetrics", e);
        } finally {
            grantBackpressureCredit();
        }
    }

    private void saveToDBAndRecordMetrics(Message msg) {
        double dbMs = eventDao.insert(msg.startTimeMs(), msg.payload()); //Insert takes about 30 ms. Because of delay_30ms in schema.sql
        log.info("Insert into DB length= {} ms", dbMs);
        // end-to-end latency
        long e2eMs = System.currentTimeMillis() - msg.startTimeMs();
        LatencyMetrics.getHistogram().recordValue(e2eMs);
        ThroughputMetrics.incrementThroughputCount();
        log.info("Latency recorded, REAL DB case: endToEnd latency={} ms, dbWrite={} ms", e2eMs, dbMs);
    }

    private void emulateWorkAndRecordMetrics(Message msg) throws InterruptedException {
        Thread.sleep(30);
        // end-to-end latency
        var e2eMs = System.currentTimeMillis() - msg.startTimeMs();
        LatencyMetrics.getHistogram().recordValue(e2eMs);
        ThroughputMetrics.incrementThroughputCount();
        log.info("Latency recorded, MOCK DB case: endToEnd latency={} ms", e2eMs);
    }

    private void grantBackpressureCredit() {
        if (backpressureEnabled) {
            backpressureGate.grant(1);
        }
    }

}