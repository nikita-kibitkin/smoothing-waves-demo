package com.example.priority.service;

import com.example.priority.metrics.LatencyMetrics;
import com.example.priority.metrics.ThroughputMetrics;
import com.example.priority.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConsumerService {
    private final DoubleQueueService dqs;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    @Value(value = "${priority-enabled}")
    private Boolean priorityEnabled;
    // Hysteresis thresholds: allow bulk only below LOW_WM; forbid bulk at/above HIGH_WM.
    @Value(value = "${poisson.low-wm}")
    private int LOW_WM;
    @Value(value = "${poisson.high-wm}")
    private int HIGH_WM;
    @Value(value = "${poisson.bulk-budget}")
    private int BULK_BUDGET; // Give bulk a few slots per tick to avoid starvation.
    private final Semaphore permits = new Semaphore(4, true);
    private volatile boolean bulkAllowed = false; // Hysteresis state

    @KafkaListener(topics = {"${spring.kafka.high-topic}"}, concurrency = "1")
    public void handleHigh(Message msg) {
        dqs.addToHigh(msg);
        // log.info("Received high-message {}", msg);
    }

    @KafkaListener(topics = {"${spring.kafka.bulk-topic}"}, concurrency = "1")
    public void handleBulk(Message msg) {
        dqs.addToBulk(msg);
        // log.info("Received bulk-message {}", msg);
    }

    @Scheduled(fixedDelay = 30)
    public void queueDispatcher() {
        // 1) Always drain high first (bounded by available permits).
        while (!dqs.isHighEmpty() && permits.tryAcquire()) {
            Message m = dqs.pollHigh();
            log.info("Polled high-message {}", m);
            if (m == null) break;
            executor.execute(() -> {
                //          log.info("Execute high-message {}", m);
                emulateWorkAndRecordMetrics(m);
                permits.release();
            });
        }

        // 2) Update hysteresis state once per tick.
        if (!bulkAllowed && dqs.getHighDepth() < LOW_WM && dqs.getHighDepth() < dqs.getBulkDepth()) {
            bulkAllowed = true;   // enable bulk only when high depth is clearly low
            log.info("Hysteresis switched to Bulk Allowed. High depth: {}. Bulk depth: {}. {} {}", dqs.getHighDepth(), dqs.getBulkDepth(), LOW_WM, HIGH_WM);

        }
        if (bulkAllowed && dqs.getHighDepth() > HIGH_WM) {
            bulkAllowed = false;  // disable bulk only when high depth is clearly high
            log.info("Hysteresis switched to Bulk Forbidden. High depth: {}. Bulk depth: {}. BULK_BUDGET: {}", dqs.getHighDepth(), dqs.getBulkDepth(), BULK_BUDGET);
        }


        // 3) If allowed, give a small, capped number of bulk slots.
        if (bulkAllowed && priorityEnabled) {
            //   log.info("Try to process bulk");
            int given = 0;
            //   log.info("BULK_BUDGET: {}", BULK_BUDGET);
            while (given < BULK_BUDGET && !dqs.isBulkEmpty() && permits.tryAcquire()) {
                Message m = dqs.pollBulk();
                log.info("Polled bulk-message {}", m);
                if (m == null) break;
                if (submitWithPermit(executor, () -> {
                            emulateWorkAndRecordMetrics(m);
                            permits.release();
                        },
                        () -> dqs.addToBulk(m))) {
                    given++;
                } else break; // pool is saturated; stop early
            }
        }
    }

    public static boolean submitWithPermit(Executor executor, Runnable task, Runnable onReject) {
        try {
            executor.execute(() -> {
                try {
                    task.run();
                } catch (Throwable t) {
                    log.error("Exception during executor.execute", t);
                }
            });
            return true;
        } catch (RejectedExecutionException rex) {
            if (onReject != null) onReject.run();  // e.g., requeue the message
            log.error("RejectedExecutionException during executor.execute", rex);
            return false;
        }
    }


    @SneakyThrows
    private void emulateWorkAndRecordMetrics(Message msg) {
        Thread.sleep(100);
        var latency = System.currentTimeMillis() - msg.startTimeMs();
        if (msg.highPriority()) {
            LatencyMetrics.getHighHistogram().recordValue(latency);
            ThroughputMetrics.incrementHigh();
        } else {
            LatencyMetrics.getBulkHistogram().recordValue(latency);
            ThroughputMetrics.incrementBulk();
        }
        log.info("Latency recorded: {} ms; {}", msg.highPriority() ? "high" : "bulk", latency);
    }

}