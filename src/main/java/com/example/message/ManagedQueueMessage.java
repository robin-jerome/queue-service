package com.example.message;

import java.util.UUID;

import lombok.Getter;

@Getter
public class ManagedQueueMessage extends QueueMessage {
    protected int priorAttemptCount;
    protected long visibleFrom;
    // Should be fetched from flavours - Lower values for testing
    public static final long VISIBILITY_TIMEOUT_MILLIS = 100;

    ManagedQueueMessage(String messageBody) {
        super(messageBody, null);
    }

    public boolean isUnread() {
        return priorAttemptCount == 0 && visibleFrom == 0L;
    }

    private boolean hasExpiredVisibility() {
        long elapsedTime = System.currentTimeMillis() - visibleFrom;
        if (elapsedTime > VISIBILITY_TIMEOUT_MILLIS) {
            System.out.println(messageBody + " is no longer invisible");
            return true;
        } else {
            System.out.println(messageBody + " is invisible for another " + (VISIBILITY_TIMEOUT_MILLIS - elapsedTime) + " ms");
            return false;
        }
    }

    public boolean isConsumable() {
        if (isUnread()) {
            return true;
        } else {
            return hasExpiredVisibility();
        }
    }

    public void markAsConsumed() {
        visibleFrom = System.currentTimeMillis();
        priorAttemptCount++;
        this.receiptId = UUID.randomUUID().toString();
        System.out.println("Consuming message with body " + this.messageBody + " and receipt id " + this.receiptId);
    }

}
