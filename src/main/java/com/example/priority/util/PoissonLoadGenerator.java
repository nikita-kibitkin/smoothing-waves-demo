package com.example.priority.util;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.TaskScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class PoissonLoadGenerator {
    private final TaskScheduler scheduler;
    private final Runnable task;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final double lambdaPerSec;
    private ScheduledFuture<?> stopFuture;
    private final ConfigurableApplicationContext ctx;

    public PoissonLoadGenerator(TaskScheduler scheduler, Runnable task, double lambdaPerSec, ConfigurableApplicationContext ctx) {
        if (lambdaPerSec <= 0) throw new IllegalArgumentException("lambda must be > 0");
        this.scheduler = scheduler;
        this.task = task;
        this.lambdaPerSec = lambdaPerSec;
        this.ctx = ctx;
        log.info("PoissonLoadGenerator initialized, l={}", lambdaPerSec);
    }

    public void start(Duration duration) {
        log.info("Starting PoissonLoadGenerator, duration={}", duration);
        if (!running.compareAndSet(false, true)) return;
        scheduleNext();
        if (duration != null) {
            stopFuture = scheduler.schedule(this::stop, Instant.now().plus(duration));
        }
    }

    public void stop() {
        running.set(false);
        if (stopFuture != null) stopFuture.cancel(false);
        log.info("PoissonLoadGenerator stopped. Wait 30s for all events to be processed");
        new Thread(() -> {
            try {
                Thread.sleep(30_000);
            } catch (InterruptedException ignored) {}
            try {
                int code = SpringApplication.exit(ctx, () -> 0);
                System.exit(code);
            } catch (Throwable t) {
                log.error("Shutdown failed, forcing System.exit(1)", t);
                System.exit(1);
            }
        }, "app-exit-thread").start();
    }

    private void scheduleNext() {
        if (!running.get()) return;
        long delayNanos = nextDelayNanos();
        scheduler.schedule(this::fireOnce, Instant.now().plusNanos(delayNanos));
    }

    private void fireOnce() {
        try {
            task.run();
        } finally {
            scheduleNext();
        }
    }

    private long nextDelayNanos() {
        double random = ThreadLocalRandom.current().nextDouble();       // (0,1)
        log.debug("random: {}, lambda: {}", random, lambdaPerSec);
        double delaySec = (-Math.log(1.0 - random)) / lambdaPerSec;       // Exp(λ)
        log.debug("PoissonLoadGenerator delaySec: {}", delaySec);
        return (long) (delaySec * 1_000_000_000L);
    }
}
