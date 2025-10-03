package com.example.smoothing.smoothing;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.*;

@Slf4j
@Service
public final class BackpressureGate {
    private final ThreadPoolExecutor executor;
    @Getter
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(2_000);
    private final Semaphore semaphore = new Semaphore(50);
    private final ScheduledExecutorService scheduler;


    public BackpressureGate(
            @Value("${backpressure.credits}") long credits,
            @Qualifier("fastScheduler") ScheduledExecutorService scheduler
    ) {
        int maxWorkers = 50;

        this.executor = new ThreadPoolExecutor(
                maxWorkers,
                100,
                0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                runnable -> {
                    Thread t = new Thread(runnable, "bp-worker");
                    t.setDaemon(true);
                    return t;
                },
                // Когда все воркеры заняты — пусть вызывающий поток сам выполнит задачу. Это даёт честный мгновенный backpressure и не накачивает очереди.
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
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




    @PostConstruct
    public void start() {
        final int PERIOD_MS = 3;
        scheduler.scheduleAtFixedRate(this::tick, 0, PERIOD_MS, TimeUnit.MILLISECONDS);
        log.info("KafkaFastTicker started with period={}ms", PERIOD_MS);
    }

    private void tick() {
        if (!semaphore.tryAcquire()) {
            return;
        }

        try {
            Runnable r = queue.poll();
            if (r == null) {
                return;
            }
            executor.execute(r);
        } catch (Throwable t) {
            log.error("Tick crashed", t);
        }finally {
            semaphore.release();
        }
    }


    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }
}
