package com.example.smoothing.generator;

import com.example.smoothing.generator.batchsize.BatchSizeSampler;
import com.example.smoothing.generator.batchsize.GeometricBatchSize;
import com.example.smoothing.metrics.ThroughputMetrics;
import com.example.smoothing.smoothing.BackpressureGate;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.TaskScheduler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
public final class StochasticLoadGenerator {
    private final TaskScheduler scheduler;
    private final TimedTask task;
    private final RateFunction rate;
    private final double lambdaMax;               // λ upper bound for thinning
    private final BatchSizeSampler batchSampler;  // k ~ D
    private final Duration intraBatchSpread;      // 0 => fire inline immediately
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConfigurableApplicationContext ctx;
    private final Clock clock;
    private final BackpressureGate gate;

    private final AtomicLong acceptedBatches = new AtomicLong();
    private final AtomicLong emittedTasks = new AtomicLong();

    private ScheduledFuture<?> stopFuture;

    @Builder
    private StochasticLoadGenerator(
            @NonNull TaskScheduler scheduler,
            @NonNull TimedTask task,
            @NonNull RateFunction rate,
            @NonNull ConfigurableApplicationContext ctx,
            BatchSizeSampler batchSampler,
            Duration intraBatchSpread,
            Clock clock,
            BackpressureGate backpressureGate, ThroughputMetrics throughputMetrics
    ) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.task = Objects.requireNonNull(task, "task");
        this.rate = Objects.requireNonNull(rate, "rate");
        this.ctx = Objects.requireNonNull(ctx, "ctx");
        this.lambdaMax = rate.upperBoundRatePerSecond();
        if (lambdaMax <= 0.0 || !Double.isFinite(lambdaMax)) {
            throw new IllegalArgumentException("rate.upperBoundRatePerSecond() must be finite and > 0");
        }
        this.batchSampler = (batchSampler != null) ? batchSampler : GeometricBatchSize.ofMean(1.0);
        int sample = this.batchSampler.sample();
        if (sample <= 0) throw new IllegalArgumentException("batchSampler must return k >= 1");
        this.intraBatchSpread = (intraBatchSpread != null) ? intraBatchSpread : Duration.ZERO;
        if (this.intraBatchSpread.isNegative()) throw new IllegalArgumentException("intraBatchSpread must be >= 0");
        this.clock = (clock != null) ? clock : Clock.systemUTC();
        this.gate = backpressureGate;
        log.info("StochasticLoadGenerator initialized: λmax={}, spread={}ms, batchDist={}, rate={}, gate={}",
                this.lambdaMax,
                this.intraBatchSpread.toMillis(),
                this.batchSampler.getClass().getSimpleName(),
                this.rate.getClass().getSimpleName(),
                this.gate);

    }

    /**
     * Start the generator for a finite duration or until stop() is called.
     */
    public void start(Duration duration) {
        log.info("Starting StochasticLoadGenerator, duration={}.", duration);
        if (!running.compareAndSet(false, true)) return;
        scheduleNextCandidate(now());
        if (duration != null) {
            stopFuture = scheduler.schedule(this::stop, now().plus(duration));
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        if (stopFuture != null) stopFuture.cancel(false);
        log.info("StochasticLoadGenerator stopping. Emitted tasks={}, batches={}.", emittedTasks.get(), acceptedBatches.get());
        new Thread(() -> {
            try {
                Thread.sleep(40_000);
            } catch (InterruptedException ignored) {
            }
            try {
                int code = SpringApplication.exit(ctx, () -> 0);
                System.exit(code);
            } catch (Throwable t) {
                log.error("Shutdown failed, forcing System.exit(1)", t);
                System.exit(1);
            }
        }, "app-exit-thread").start();
    }

    // === Core NHPP (Lewis–Shedler thinning) ===

    private void scheduleNextCandidate(Instant baseTime) {
        if (!running.get()) return;
        long delayNanos = sampleExpNanos(lambdaMax);
        Instant candidateAt = baseTime.plusNanos(delayNanos);
        scheduler.schedule(() -> tryAcceptAt(candidateAt), candidateAt);
    }

    private void tryAcceptAt(Instant candidateAt) {
        if (!running.get()) return;
        double lambdaHere = Math.max(0.0, rate.ratePerSecond(candidateAt));
        if (!Double.isFinite(lambdaHere)) lambdaHere = 0.0;
        double acceptP = Math.min(1.0, lambdaHere / lambdaMax);
        double u = ThreadLocalRandom.current().nextDouble();
        if (u < acceptP) {
            // Accepted arrival — emit a compound batch
            int k = Math.max(1, batchSampler.sample());
            acceptedBatches.incrementAndGet();
            emitBatch(k, candidateAt);
        }
        // Whether accepted or not, schedule next candidate measured from this candidate time (memoryless property)
        scheduleNextCandidate(candidateAt);
    }

    private void emitBatch(int k, Instant t0) {
        if (intraBatchSpread.isZero() || k == 1) {
            for (int i = 0; i < k; i++) safeRun();
            emittedTasks.addAndGet(k);
            return;
        }
        long spanNanos = Math.max(0, intraBatchSpread.toNanos());
        long step = (k <= 1) ? spanNanos : spanNanos / k;
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        for (int i = 0; i < k; i++) {
            long jitter = (step > 0) ? tlr.nextLong(0, Math.max(1, step)) : 0L;
            Instant when = t0.plusNanos((long) i * step + jitter);
            scheduler.schedule(() -> {
                safeRun();
                emittedTasks.incrementAndGet();
            }, when);
        }
    }

    private void safeRun() {
        try {
            final long t0 = System.currentTimeMillis(); // Created time
            if (gate == null) {
                task.run(t0);
            } else {
                gate.enqueue(() -> task.run(t0));
            }
        } catch (Throwable t) {
            log.error("task.run() failed", t);
        }
        ThroughputMetrics.incrementIngressRateCount();
    }

    private long sampleExpNanos(double lambdaPerSec) {
        // Exp(λ): -ln(U)/λ (U~Uniform(0,1))
        double u = ThreadLocalRandom.current().nextDouble();
        double delaySec = (-Math.log(1.0 - u)) / lambdaPerSec;
        return (long) (delaySec * 1_000_000_000L);
    }

    private Instant now() {
        return clock.instant();
    }

    public long getEmittedTasks() {
        return emittedTasks.get();
    }
}
