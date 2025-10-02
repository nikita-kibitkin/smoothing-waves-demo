package com.example.smoothing.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class SchedulerConfig {


    @Bean
    public TaskScheduler taskScheduler() {
        var s = new ThreadPoolTaskScheduler();
        s.setPoolSize(1);                 // под себя
        s.setThreadNamePrefix("sched-");
        s.initialize();
        return s;
    }

    @Bean(name = "fastScheduler", destroyMethod = "shutdown")
    public ScheduledThreadPoolExecutor fastScheduler() {
        var tf = new ThreadFactory() {
            final AtomicInteger idx = new AtomicInteger(1);
            @Override public Thread newThread(Runnable r) {
                var t = new Thread(r, "fast-sched-" + idx.getAndIncrement());
                t.setDaemon(true);
                t.setPriority(Thread.NORM_PRIORITY + 1);
                return t;
            }
        };

        var exec = new ScheduledThreadPoolExecutor(1, tf, new ThreadPoolExecutor.DiscardPolicy()); // не буферизуем лишнее
        exec.setRemoveOnCancelPolicy(true);
        exec.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return exec;
    }
}