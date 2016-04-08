package com.example.service;

import static junit.framework.TestCase.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.example.message.ManagedQueueMessage;
import com.example.message.QueueMessage;

public class GenericQueueServiceTest {
    protected QueueService service;
    protected static final String TEST_QUEUE_NAME = "myTestQueue";
    protected static final String MESSAGE_BODY = "testMsg";
    protected Map<String, String> receiptQueueNameMap = new HashMap<>();

    public GenericQueueServiceTest(QueueService service) {
        this.service = service;
    }

    public void setUp() {
        receiptQueueNameMap.clear();
    }

    public void tearDown() {
        Map<String, String> staleEntriesInQueue = new HashMap<>();
        receiptQueueNameMap.values().stream().distinct().forEach(queueName -> {
            Optional<QueueMessage> msgOpt = service.pull(queueName);
            if (msgOpt.isPresent()) {
                staleEntriesInQueue.put(msgOpt.get().getReceiptId(), queueName);
            }
        });
        receiptQueueNameMap.forEach((r, q) -> service.delete(q, r));
        staleEntriesInQueue.forEach((r, q) -> service.delete(q, r));
    }

    protected void pushedItemsAreRetrievedByPull() throws Exception {
        service.push(TEST_QUEUE_NAME, MESSAGE_BODY);
        Optional<QueueMessage> msgOpt = service.pull(TEST_QUEUE_NAME);
        String receiptId = msgOpt.get().getReceiptId();
        receiptQueueNameMap.put(receiptId, TEST_QUEUE_NAME);
        assertTrue(msgOpt.isPresent());
        assertEquals(MESSAGE_BODY, msgOpt.get().getMessageBody());
        assertNotNull(msgOpt.get().getReceiptId());
    }

    protected void pushedItemsAreRetrievedInOrder() throws Exception {
        for (int i = 0; i < 5; i++) {
            service.push(TEST_QUEUE_NAME, MESSAGE_BODY + ":" + i);
        }
        for (int i = 0; i < 5; i++) {
            Optional<QueueMessage> msgOpt = service.pull(TEST_QUEUE_NAME);
            String receiptId = msgOpt.get().getReceiptId();
            receiptQueueNameMap.put(receiptId, TEST_QUEUE_NAME);
            assertTrue(msgOpt.isPresent());
            assertEquals(MESSAGE_BODY + ":" + i, msgOpt.get().getMessageBody());
            assertNotNull(msgOpt.get().getReceiptId());
        }
    }

    protected void pulledItemsBecomeVisibleAfterTimeout() throws Exception {
        service.push(TEST_QUEUE_NAME, MESSAGE_BODY);
        assertTrue(service.pull(TEST_QUEUE_NAME).isPresent());
        // A received message is not received again on calling immediately
        assertFalse(service.pull(TEST_QUEUE_NAME).isPresent());
        Thread.sleep(ManagedQueueMessage.VISIBILITY_TIMEOUT_MILLIS + 1);
        Optional<QueueMessage> msgOpt = service.pull(TEST_QUEUE_NAME);
        assertTrue(msgOpt.isPresent());
        String receiptId = msgOpt.get().getReceiptId();
        receiptQueueNameMap.put(receiptId, TEST_QUEUE_NAME);
    }

    protected void pullFromNonExistingQueueThrowsException() {
        service.pull(TEST_QUEUE_NAME + System.currentTimeMillis());
    }

    protected void deleteFromNonExistingQueueThrowsException() {
        service.delete(TEST_QUEUE_NAME + System.currentTimeMillis(), "" + System.currentTimeMillis());
    }

    protected void pushingToNonExistentQueueCreatesQueue() throws Exception {
        String randomQueueName = TEST_QUEUE_NAME + System.currentTimeMillis();
        service.push(randomQueueName, MESSAGE_BODY);
        Optional<QueueMessage> msgOpt = service.pull(randomQueueName);
        assertTrue(msgOpt.isPresent());
        String receiptId = msgOpt.get().getReceiptId();
        receiptQueueNameMap.put(receiptId, randomQueueName);
    }

}
