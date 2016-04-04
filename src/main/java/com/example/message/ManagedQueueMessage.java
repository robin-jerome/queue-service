package com.example.message;

import java.util.Date;

import lombok.Setter;

@Setter
public class ManagedQueueMessage extends QueueMessage {
    private int priorAttemptCount;
    private Date visibleFrom;
}
