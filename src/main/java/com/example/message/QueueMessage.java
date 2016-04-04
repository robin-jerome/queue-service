package com.example.message;

import java.io.Serializable;

import lombok.Getter;

@Getter
public class QueueMessage implements Serializable, Cloneable {
    protected String receiptId;
    protected String messageBody;

    QueueMessage(String messageBody, String receiptId) {
        this.messageBody = messageBody;
        this.receiptId = receiptId;
    }

    @Override
    public QueueMessage clone() {
        try {
            return (QueueMessage) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(
                    "Got a CloneNotSupportedException from Object.clone() "
                            + "even though we're Cloneable!", e);
        }
    }
}
