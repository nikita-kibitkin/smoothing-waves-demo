package com.example.priority.service;

import com.example.priority.model.Message;

//Contract. In memory queues implementation can be replaced with some enterprise solution.
public interface DoubleQueueService {

    void addToHigh(Message message);

    void addToBulk(Message message);

    Message pollHigh();

    Message pollBulk();

    int getHighDepth();

    int getBulkDepth();

    boolean isHighEmpty();

    boolean isBulkEmpty();
}
