package com.example.smoothing.smoothing;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
@RequiredArgsConstructor
public final class BackpressureGate {
    private final ExecutorService executor = Executors.newFixedThreadPool(20);
    @Getter
    private final java.util.concurrent.atomic.AtomicLong credits = new java.util.concurrent.atomic.AtomicLong(100);
    @Getter
    private final java.util.concurrent.BlockingQueue<Runnable> queue = new java.util.concurrent.LinkedBlockingQueue<>(100_000);

    public void enqueue(Runnable r) {
        boolean ok = queue.offer(r);
        if (!ok) {
            try {
                queue.put(r);
            } catch (InterruptedException ignored) {
                log.error("queue interrupted");
            }
        }
        drain();
    }

    public void grant(long n) { // called by downstream (handler)
        if (n > 0) {
            credits.addAndGet(n);
            drain();
        }
    }

    private void drain() {
        while (true) {
            if (!tryAcquireCredit()) break;             // no credits → break
            Runnable r = queue.poll();                  // trying to take task
            if (r == null) {                            // no task → credit back
                credits.incrementAndGet();
                break;
            }
            executor.execute(r);                        // got a task → execute the task and spend credit
        }
    }

    private boolean tryAcquireCredit() {
        long credits;
        do {
            credits = this.credits.get();
            if (credits <= 0) return false;
        } while (!this.credits.compareAndSet(credits, credits - 1)); // atomic credit acquire
        return true;
    }
}

