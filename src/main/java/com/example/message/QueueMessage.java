package com.example.message;

import java.io.Serializable;

import lombok.Getter;

@Getter
public class QueueMessage implements Serializable {
    protected String receiptId;
    protected String messageBody;

    QueueMessage(String messageBody, String receiptId) {
        this.messageBody = messageBody;
        this.receiptId = receiptId;
    }

    public QueueMessage copy() {
        return new QueueMessage(messageBody, receiptId);
    }
}
