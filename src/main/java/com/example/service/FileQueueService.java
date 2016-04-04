package com.example.service;

import java.util.Optional;

import com.example.message.QueueMessage;import com.example.message.QueueMessage;

public class FileQueueService implements QueueService {
    @Override
    public void push(String queueName, String message) {

    }

    @Override
    public Optional<QueueMessage> pull(String queueName) {
        return null;
    }

    @Override
    public void delete(String queueName, String messageReceipt) {

    }
    //
    // Task 3: Implement me if you have time.
    //
}
