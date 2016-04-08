package com.example.service;

import com.example.message.ManagedQueueMessage;
import com.example.message.QueueMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static junit.framework.TestCase.*;

public class GenericQueueServiceTest {
    protected QueueService service;
    protected static final String TEST_QUEUE_NAME = "myTestQueue";
    protected static final String MESSAGE_BODY = "testMsg";
    protected List<String> receiptIds = new ArrayList<>();

    public GenericQueueServiceTest(QueueService service) {
        this.service = service;
    }

    public void setUp() {
        receiptIds.clear();
    }

    public void tearDown() {
        receiptIds.forEach(r -> service.delete(TEST_QUEUE_NAME, r));
    }

    protected void pushedItemsAreRetrievedByPull() throws Exception {
        service.push(TEST_QUEUE_NAME, MESSAGE_BODY);
        Optional<QueueMessage> msgOpt = service.pull(TEST_QUEUE_NAME);
        String receiptId = msgOpt.get().getReceiptId();
        receiptIds.add(receiptId);
        assertTrue(TEST_QUEUE_NAME, msgOpt.isPresent());
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
            receiptIds.add(receiptId);
            assertTrue(TEST_QUEUE_NAME, msgOpt.isPresent());
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
        receiptIds.add(receiptId);
    }

    protected void pullFromNonExistingQueueThrowsException() {
        service.pull(TEST_QUEUE_NAME + System.currentTimeMillis());
    }
}
