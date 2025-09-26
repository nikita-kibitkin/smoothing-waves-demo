package com.example.smoothing.generator;

@FunctionalInterface
public interface TimedTask {
    void run(long createdTime);
}
