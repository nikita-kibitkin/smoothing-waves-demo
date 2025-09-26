package com.example.smoothing.metrics;

import com.example.smoothing.service.ProducerService;
import com.example.smoothing.smoothing.BackpressureGate;
import io.micrometer.core.instrument.Gauge;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class GaugeRegistry {
    private final io.micrometer.core.instrument.MeterRegistry registry;
    private final BackpressureGate backpressureGate;
    private final ProducerService producerService;

    @PostConstruct
    void init() {
        //throughput
        Gauge.builder("throughput", ThroughputMetrics::getCurrentThroughput)
                .register(registry);
        Gauge.builder("ingressRate", ThroughputMetrics::getCurrentIngressRate)
                .register(registry);
        //count total
        Gauge.builder("count.emitted", () -> producerService.getStochasticGenerator().getEmittedTasks())
                .register(registry);
        Gauge.builder("count.handled", ThroughputMetrics::getThroughputTotalCount)
                .register(registry);
        //latency
        Gauge.builder("latency.avg", () -> LatencyMetrics.getHistogramCopy().getMean())
                .register(registry);
        Gauge.builder("latency.p99", () -> LatencyMetrics.getHistogramCopy().getValueAtPercentile(99))
                .register(registry);
        Gauge.builder("latency.p95", () -> LatencyMetrics.getHistogramCopy().getValueAtPercentile(95))
                .register(registry);
        //backpressureGate queue depth
        Gauge.builder("backpressureGate.queue.depth", () -> backpressureGate.getQueue().size())
                .register(registry);
        //backpressureGate credits
        Gauge.builder("backpressureGate.credits", backpressureGate::getCredits)
                .register(registry);
    }
}