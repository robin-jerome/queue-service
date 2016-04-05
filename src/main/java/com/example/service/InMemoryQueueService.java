package com.example.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.example.message.ManagedQueueMessage;
import com.example.message.QueueMessage;
import com.example.message.QueueMessageFactory;

public class InMemoryQueueService implements QueueService {
    Map<String, ConcurrentLinkedQueue<ManagedQueueMessage>> queues = new HashMap<>();

    @Override
    public void push(String queueName, String message) {
        if (queues.containsKey(queueName)) {
            queues.get(queueName).add(QueueMessageFactory.createManagedQueueMessage(message));
        } else {
            ConcurrentLinkedQueue myQueue = new ConcurrentLinkedQueue<>();
            myQueue.add(QueueMessageFactory.createManagedQueueMessage(message));
            queues.put(queueName, myQueue);
        }
    }

    @Override
    public Optional<QueueMessage> pull(String queueName) {
        if (queues.containsKey(queueName)) {
            Optional<ManagedQueueMessage> optionalMessage = queues.get(queueName).stream()
                    .filter(ManagedQueueMessage::isConsumable)
                    .findFirst();

            if (optionalMessage.isPresent()) {
                ManagedQueueMessage message = optionalMessage.get();
                message.markAsConsumed();
                return Optional.of(message.copy());
            } else {
                return Optional.empty();
            }
        } else {
            throw new RuntimeException("Queue with name " + queueName + " does not exist");
        }
    }

    @Override
    public void delete(String queueName, String messageReceipt) {
        if (queues.containsKey(queueName)) {
            Optional<ManagedQueueMessage> optionalMessage = queues.get(queueName).stream()
                    .filter(m -> !m.isUnread() && m.getReceiptId().equals(messageReceipt))
                    .findFirst();
            if (optionalMessage.isPresent()) {
                queues.get(queueName).remove(optionalMessage.get());
            } else {
                throw new RuntimeException("Message with receipt " + messageReceipt + " does not exist in queue " + queueName);
            }
        } else {
            throw new RuntimeException("Queue with name " + queueName + " does not exist");
        }
    }

}
