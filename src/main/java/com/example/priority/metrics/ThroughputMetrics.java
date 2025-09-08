package com.example.priority.metrics;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.LongAdder;

@Slf4j
@Component
public class ThroughputMetrics {
    private final static LongAdder countHigh = new LongAdder();
    private final static LongAdder countBulk = new LongAdder();
    private final int fixedRate = 5; //Throughput window in seconds,
    @Getter
    private static volatile double currentThroughputHigh;
    @Getter
    private static volatile double currentThroughputBulk;

    public static void incrementHigh() {
        countHigh.increment();
    }

    public static void incrementBulk() {
        countBulk.increment();
    }

    @Scheduled(fixedRate = fixedRate * 1000)
    public void recordAndReset() {
        double throughputHigh = (double) countHigh.sum() / fixedRate;
        double throughputBulk = (double) countBulk.sum() / fixedRate;

        currentThroughputHigh = throughputHigh;
        currentThroughputBulk = throughputBulk;
        countHigh.reset();
        countBulk.reset();
        log.info("Throughput recorded for last {} ms. ThroughputHigh={}. ThroughputBulk={}", fixedRate, throughputHigh, throughputBulk);
    }
}
