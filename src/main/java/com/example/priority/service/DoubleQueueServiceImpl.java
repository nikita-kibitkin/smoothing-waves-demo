package com.example.priority.service;

import com.example.priority.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ArrayBlockingQueue;

@Slf4j
@Service
@RequiredArgsConstructor
//In memory queues implementation can be replaced with some enterprise solution.
public class DoubleQueueServiceImpl implements DoubleQueueService {
    private final ArrayBlockingQueue<Message> highQueue = new ArrayBlockingQueue<>(100_000);
    private final ArrayBlockingQueue<Message> bulkQueue = new ArrayBlockingQueue<>(100_000);

    @Override
    public void addToHigh(Message message) {
        highQueue.add(message);
    }

    @Override
    public void addToBulk(Message message) {
        bulkQueue.add(message);
    }

    @Override
    public Message pollHigh() {
        return highQueue.poll();
    }

    @Override
    public Message pollBulk() {
        return bulkQueue.poll();
    }

    @Override
    public int getHighDepth() {
        return highQueue.size();
    }

    @Override
    public int getBulkDepth() {
        return bulkQueue.size();
    }

    @Override
    public boolean isHighEmpty() {
        return highQueue.isEmpty();
    }

    @Override
    public boolean isBulkEmpty() {
        return bulkQueue.isEmpty();
    }
}
