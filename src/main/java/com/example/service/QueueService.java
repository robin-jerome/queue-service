package com.example.service;

import java.util.Optional;

import com.example.message.QueueMessage;

public interface QueueService {

    void push(String queueName, String message);

    Optional<QueueMessage> pull(String queueName);

    void delete(String queueName, String messageReceipt);

}
