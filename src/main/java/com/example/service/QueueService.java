package com.example.service;

import java.util.Optional;

import com.example.message.QueueMessage;
import com.example.message.QueueMessageFactory;

public interface QueueService {

    //
    // Task 1: Define me.
    //
    // This interface should include the following methods.  You should choose appropriate
    // signatures for these methods that prioritise simplicity of implementation for the range of
    // intended implementations (in-memory, file, and SQS).  You may include additional methods if
    // you choose.
    //
    // - push
    //   pushes a message onto a queue.
    // - pull
    //   retrieves a single message from a queue.
    // - delete
    //   deletes a message from the queue that was received by pull().
    //

    void push(String queueName, String message);

    Optional<QueueMessage> pull(String queueName);

    void delete(String queueName, String messageReceipt);

}
