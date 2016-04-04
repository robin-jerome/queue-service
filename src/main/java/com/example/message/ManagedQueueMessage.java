package com.example.message;

import java.util.UUID;

import org.joda.time.DateTime;

import lombok.Getter;

@Getter
public class ManagedQueueMessage extends QueueMessage {
    protected int priorAttemptCount = 0;
    protected DateTime visibleFrom;
    // Should be fetched from flavours
    private static final long VISIBILITY_TIMEOUT_MS = 2000;

    ManagedQueueMessage(String messageBody,
                        String receiptId,
                        int priorAttemptCount,
                        DateTime visibleFrom) {
        super(messageBody, receiptId);
        this.priorAttemptCount = priorAttemptCount;
        this.visibleFrom = visibleFrom;
    }

    ManagedQueueMessage(String messageBody) {
        super(messageBody, null);
    }

    public boolean isUnread() {
        return priorAttemptCount == 0 && visibleFrom == null;
    }

    public boolean isInvisible() {
        return !isUnread() && visibleFrom.plus(VISIBILITY_TIMEOUT_MS).isBeforeNow();
    }

    public boolean isConsumable() {
        return isUnread() || !isInvisible();
    }

    public void markAsConsumed() {
        visibleFrom = DateTime.now();
        priorAttemptCount++;
        this.receiptId = UUID.randomUUID().toString();
    }

}
