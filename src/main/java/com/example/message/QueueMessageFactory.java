package com.example.message;

public class QueueMessageFactory {

    public static QueueMessage convertToQueueMessage(String messageBody,
                                                     String receiptId) {
        return new QueueMessage(messageBody, receiptId);
    }

    public static ManagedQueueMessage createManagedQueueMessage(String messageBody) {
        return new ManagedQueueMessage(messageBody);
    }

}
