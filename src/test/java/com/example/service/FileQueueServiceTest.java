package com.example.service;

import static junit.framework.TestCase.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.After;
import org.junit.Test;

import com.example.message.ManagedQueueMessage;
import com.example.message.QueueMessage;

public class FileQueueServiceTest {
    private QueueService service = new FileQueueService();
    private static final String TEST_QUEUE_NAME = "myTestQueue";
    private static final String MESSAGE_BODY = "testMsg";
    private List<String> receiptIds = new ArrayList<>();

    @After
    public void setUp() {
        receiptIds.clear();
    }

    @After
    public void tearDown() {
        receiptIds.forEach(r -> service.delete(TEST_QUEUE_NAME, r));
    }


    @Test
    public void pushedItemsAreRetrievedByPull() throws Exception {
        service.push(TEST_QUEUE_NAME, MESSAGE_BODY);
        Optional<QueueMessage> msgOpt = service.pull(TEST_QUEUE_NAME);
        String receiptId = msgOpt.get().getReceiptId();
        receiptIds.add(receiptId);
        assertTrue(TEST_QUEUE_NAME, msgOpt.isPresent());
        assertEquals(MESSAGE_BODY, msgOpt.get().getMessageBody());
        assertNotNull(msgOpt.get().getReceiptId());
    }

    @Test
    public void pushedItemsAreRetrievedInOrder() throws Exception {
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

    @Test
    public void pulledItemsBecomeVisibleAfterTimeout() throws Exception {
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

}
