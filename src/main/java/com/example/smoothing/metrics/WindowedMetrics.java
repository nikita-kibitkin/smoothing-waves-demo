package com.example.smoothing.metrics;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
@Component
public class WindowedMetrics {
    private final int fixedRate = 1; //Throughput window in seconds,
    @Getter
    private final static LongAdder throughputTotalCount = new LongAdder();
    private final static LongAdder throughputWindowCount = new LongAdder();
    @Getter
    private static volatile double currentThroughput;
    private static final LongAdder ingressWindowCount = new LongAdder();
    @Getter
    private static volatile double currentIngressRate;

    private static final DoubleAdder latencyWindowSum = new DoubleAdder();
    @Getter
    private static volatile double currentLatencyWindowAvg;




    public static void recordMetrics(Long e2eLatency) {
        throughputTotalCount.increment();
        throughputWindowCount.increment();
        latencyWindowSum.add(e2eLatency);
    }

    public static void incrementIngressRateCount() {
        ingressWindowCount.increment();
    }


    @Scheduled(fixedRate = fixedRate * 1000)
    public void recordAndReset() {
        double throughput = (double) throughputWindowCount.sum() / fixedRate;
        currentThroughput = throughput;
        throughputWindowCount.reset();
        log.info("Throughput recorded for last {} s. Throughput={}.", fixedRate, throughput);

        double ingressRate = (double) ingressWindowCount.sum() / fixedRate;
        currentIngressRate = ingressRate;
        ingressWindowCount.reset();
        log.info("IngressRate recorded for last {} s. IngressRate={}.", fixedRate, ingressRate);

        double latencyWindowAvg = latencyWindowSum.sum() / fixedRate;
        currentLatencyWindowAvg = latencyWindowAvg;
        latencyWindowSum.reset();
        log.info("LatencyWindowAvg recorded for last {} s.LatencyWindowAvg={}.", fixedRate, latencyWindowAvg);
    }
}
