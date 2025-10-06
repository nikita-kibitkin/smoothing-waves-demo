package com.example.smoothing.generator;

import com.example.smoothing.model.Message;
import org.springframework.kafka.support.Acknowledgment;

@FunctionalInterface
public interface TimedTaskDB {
    void run(Message msg, Acknowledgment ack);
}

