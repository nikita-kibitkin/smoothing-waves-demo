package com.example.smoothing.service;

import com.example.smoothing.db.EventDao;
import com.example.smoothing.generator.TimedTaskDB;
import com.example.smoothing.metrics.HistogramMetrics;
import com.example.smoothing.metrics.WindowedMetrics;
import com.example.smoothing.model.Message;
import com.example.smoothing.smoothing.BackpressureGate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

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

    private final TimedTaskDB dbInsertTask = (Message msg, Acknowledgment ack) -> {
        try {
            if (dbEnabled) {
                saveToDBAndRecordMetrics(msg);
            } else {
                emulateWorkAndRecordMetrics(msg);
            }
            //Thread.sleep(2);
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            grantBackpressureCredit();
        }
        ack.acknowledge();
    };


    @KafkaListener(topics = {"${spring.kafka.topic}"}, concurrency = "1")
    public void handle(Message msg, Acknowledgment ack) {
        try {
            if (backpressureEnabled) {
                backpressureGate.enqueue(() -> dbInsertTask.run(msg, ack));
            }else {
                dbInsertTask.run(msg, ack);
            }
        } catch (Exception e) {
            log.error("Error in ConsumerService.handle()", e);
        }
    }

    private void saveToDBAndRecordMetrics(Message msg) {
        //double dbMs = eventDao.insert(msg.startTimeMs(), msg.payload()); //Insert takes about 30 ms. Because of delay_30ms in schema.sql
        double dbMs1 = eventDao.insert(msg.startTimeMs(), msg.payload()); //Insert takes about 30 ms. Because of delay_30ms in schema.sql
        double dbMs2 = eventDao.insert(msg.startTimeMs(), msg.payload());
        //log.info("Insert into DB length= {} ms", dbMs);
        // end-to-end latency
        long e2eMs = System.currentTimeMillis() - msg.startTimeMs();
        HistogramMetrics.getHistogram().recordValue(e2eMs);
        WindowedMetrics.recordMetrics(e2eMs);
        if (e2eMs > 100_000) {
            log.warn("LARGE Latency recorded, REAL DB case: endToEnd latency={} ms, dbWrite={} ms, startTime={}",
                    e2eMs, dbMs1 + dbMs2, LocalDateTime.ofInstant(Instant.ofEpochMilli(msg.startTimeMs()), ZoneId.of("Asia/Ho_Chi_Minh")));
        }
    }

    private void emulateWorkAndRecordMetrics(Message msg) {
        //Thread.sleep(1);
        // end-to-end latency
        var e2eMs = System.currentTimeMillis() - msg.startTimeMs();
        HistogramMetrics.getHistogram().recordValue(e2eMs);
        WindowedMetrics.recordMetrics(e2eMs);
        log.info("Latency recorded, MOCK DB case: endToEnd latency={} ms", e2eMs);
    }

    private void grantBackpressureCredit() {
        if (backpressureEnabled) {
            backpressureGate.grant(1);
        }
    }

}