package com.example.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.example.message.ManagedQueueMessage;import com.example.message.QueueMessage;import com.example.message.ManagedQueueMessage;
import com.example.message.QueueMessage;

public class InMemoryQueueService implements QueueService {
    Map<String, ConcurrentLinkedQueue<ManagedQueueMessage>> queues = new HashMap<>();

    @Override
    public void push(String queueName, String message) {
        if (queues.containsKey(queueName)) {

        } else {
            ConcurrentLinkedQueue myQueue = new ConcurrentLinkedQueue<>();
            myQueue.add(new ManagedQueueMessage());
            queues.put(queueName, myQueue);
        }
    }

    @Override
    public Optional<QueueMessage> pull(String queueName) {
        return null;
    }

    @Override
    public void delete(String queueName, String messageReceipt) {

    }


}
