package com.example.smoothing.smoothing;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public final class BackpressureGate {
    @Getter
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(10_000);
    private final Semaphore semaphore = new Semaphore(1);
    private final ScheduledExecutorService scheduler;
    @Getter
    private final AtomicLong credits;

    public BackpressureGate(
            @Value("${backpressure.credits}") long credits,
            @Qualifier("fastScheduler") ScheduledExecutorService scheduler
    ) {
        this.credits = new AtomicLong(credits);
        this.scheduler = scheduler;
    }

    public void enqueue(Runnable r) {
        boolean ok = queue.offer(r);
        if (!ok) {
            try {
                queue.put(r);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("queue interrupted", e);
            }
        }
    }

    public void grant(long n) {
        if (n > 0) {
            credits.addAndGet(n);
        }
    }


    @PostConstruct
    public void start() {
        final int PERIOD_MCS = 1000;
        scheduler.scheduleAtFixedRate(this::tick, 0, PERIOD_MCS, TimeUnit.MICROSECONDS);
        log.info("KafkaFastTicker started with period={}ms", PERIOD_MCS);
    }

    private void tick() {
        if (queue.isEmpty() || credits.get() <= 0) {
            return;
        }

        try {
            if(!semaphore.tryAcquire() || !tryAcquireCredit()) return;
            Runnable r = queue.poll();
            if (r == null) {
                return;
            }
            r.run();
            semaphore.release();
        } catch (Throwable t) {
            log.error("Tick crashed", t);
            semaphore.release();
        }
    }
    private boolean tryAcquireCredit() {
        while (true) {
            long c = credits.get();
            if (c <= 0) return false;
            if (credits.compareAndSet(c, c - 1)) return true;
        }
    }


}
