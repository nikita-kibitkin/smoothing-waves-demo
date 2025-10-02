package com.example.smoothing.smoothing;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
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
    private final ThreadPoolExecutor executor;
    @Getter
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(20_000);
    @Getter
    private final AtomicLong credits;
    private final Semaphore semaphore = new Semaphore(50);
    private final ScheduledExecutorService scheduler;


    public BackpressureGate(
            @Value("${backpressure.credits}") long credits,
            @Qualifier("fastScheduler") ScheduledExecutorService scheduler
    ) {
        this.credits = new AtomicLong(credits);
        int maxWorkers = 200;

        this.executor = new ThreadPoolExecutor(
                maxWorkers,
                maxWorkers,
                0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                runnable -> {
                    Thread t = new Thread(runnable, "bp-worker");
                    t.setDaemon(true);
                    return t;
                },
                // Когда все воркеры заняты — пусть вызывающий поток сам выполнит задачу
                // Это даёт честный мгновенный backpressure и не накачивает очереди.
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
                return;
            }
        }
//        if (semaphore.tryAcquire()){
//            drain();
//        }
    }

//    public void grant(long n) {
//        if (n > 0) {
//            credits.addAndGet(n);
//
//            if (semaphore.tryAcquire()){
//                drain();
//            }
//        }
//    }

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


//    private void drain() {
//        try {
//            while (true) {
//                if (!tryAcquireCredit()) break;             // no credits → break
//                Runnable r = queue.poll();                  // trying to take task
//                if (r == null) {                            // no task → credit back
//                    credits.incrementAndGet();
//                    break;
//                }
//                executor.execute(r);                        // got a task → execute the task and spend credit
//            }
//        }catch (Exception e) {
//            log.error("Error in drain()", e);
//        }finally {
//            semaphore.release();
//        }
//    }

    private boolean tryAcquireCredit() {
        while (true) {
            long c = credits.get();
            if (c <= 0) return false;
            if (credits.compareAndSet(c, c - 1)) return true;
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }
}
