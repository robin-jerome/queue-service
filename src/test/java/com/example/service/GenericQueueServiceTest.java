package com.example.service;

import static junit.framework.TestCase.*;

import java.util.Optional;

import com.example.message.ManagedQueueMessage;
import com.example.message.QueueMessage;

public class GenericQueueServiceTest {
    protected QueueService service;
    protected static final String TEST_QUEUE_NAME = "myTestQueue";
    protected static final String MESSAGE_BODY = "testMsg";

    public GenericQueueServiceTest(QueueService service) {
        this.service = service;
    }

    protected void pushedItemsAreRetrievedByPull() throws Exception {
        service.push(TEST_QUEUE_NAME, MESSAGE_BODY);
        Optional<QueueMessage> msgOpt = service.pull(TEST_QUEUE_NAME);
        assertValidMessage(msgOpt, MESSAGE_BODY);
    }

    protected void pushedItemsAreRetrievedInOrder() throws Exception {
        for (int i = 0; i < 5; i++) {
            service.push(TEST_QUEUE_NAME, MESSAGE_BODY + ":" + i);
        }
        for (int i = 0; i < 5; i++) {
            Optional<QueueMessage> msgOpt = service.pull(TEST_QUEUE_NAME);
            assertValidMessage(msgOpt, MESSAGE_BODY + ":" + i);
        }
    }

    protected void pulledItemsBecomeVisibleAfterTimeout() throws Exception {
        service.push(TEST_QUEUE_NAME, MESSAGE_BODY);
        assertValidMessage(service.pull(TEST_QUEUE_NAME), MESSAGE_BODY);
        // A received message is not received again on calling immediately
        assertFalse(service.pull(TEST_QUEUE_NAME).isPresent());
        Thread.sleep(ManagedQueueMessage.VISIBILITY_TIMEOUT_MILLIS + 10);
        // Invisible message becomes visible after timeout
        assertValidMessage(service.pull(TEST_QUEUE_NAME), MESSAGE_BODY);
    }

    protected void pullFromNonExistingQueueThrowsException() {
        String nonExistingQueue = TEST_QUEUE_NAME + System.currentTimeMillis();
        service.pull(nonExistingQueue);
    }

    protected void deleteFromNonExistingQueueThrowsException() {
        String nonExistingQueue = TEST_QUEUE_NAME + System.currentTimeMillis();
        String nonExistingReceipt = "" + System.currentTimeMillis();
        service.delete(nonExistingQueue, nonExistingReceipt);
    }

    protected void pushingToNonExistentQueueCreatesQueue() throws Exception {
        String nonExistingQueue = TEST_QUEUE_NAME + System.currentTimeMillis();
        service.push(nonExistingQueue, MESSAGE_BODY);
        assertValidMessage(service.pull(nonExistingQueue), MESSAGE_BODY);
    }

    protected void deletingNonExistingMessageThrowsException() throws Exception {
        // Create Queue by pushing a message
        service.push(TEST_QUEUE_NAME, MESSAGE_BODY);
        String nonExistingReceipt = "" + System.currentTimeMillis();
        service.delete(TEST_QUEUE_NAME, nonExistingReceipt);
    }

    private void assertValidMessage(Optional<QueueMessage> msgOpt, String messageBody) {
        assertTrue("Message was expected but not received", msgOpt.isPresent());
        assertEquals("Message bodies do not match", messageBody, msgOpt.get().getMessageBody());
        assertNotNull("Message receipt is invalid", msgOpt.get().getReceiptId());
    }
}
