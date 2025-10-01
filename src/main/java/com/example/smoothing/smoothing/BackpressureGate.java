package com.example.smoothing.smoothing;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public final class BackpressureGate {
    // === ИСПОЛНИТЕЛЬ ===
    // Пул без внутренней очереди: прямой hand-off. Нет «второго буфера».
    private final ThreadPoolExecutor executor;
    @Getter
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(20_000);
    @Getter
    private final AtomicLong credits;
    // Защита от параллельных drain(): один активный дренёр
    private final AtomicInteger wip = new AtomicInteger(0);

    public BackpressureGate(
            @Value("${backpressure.credits}") long credits
    ) {
        this.credits = new AtomicLong(credits);
        // === НАСТРОЙКИ ===
        // Сколько одновременно воркеров реально выполняют задачи.
        // Подстрой под пул соединений БД. Для твоей машины начни с 64..128.
        int maxWorkers = 300;

        this.executor = new ThreadPoolExecutor(
                maxWorkers,
                maxWorkers,
                0L, TimeUnit.MILLISECONDS,
                // НИКАКИХ очередей внутри пула — иначе снова будет двойное буферизование
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
        drain();
    }

    public void grant(long n) {
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
