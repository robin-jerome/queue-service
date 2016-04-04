package com.example.service;

import com.example.message.QueueMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static junit.framework.TestCase.*;


public class InMemoryQueueServiceTest {
    private QueueService service = new InMemoryQueueService();
    private static final String TEST_QUEUE_NAME = "myTestQueue";
    private static final String MESSAGE_BODY = "testMsg";
    private String receiptId;

    @Before
    public void setUp() {
        service.push(TEST_QUEUE_NAME, MESSAGE_BODY);
    }

    @After
    public void tearDown() {
        if (receiptId != null) {
            service.delete(TEST_QUEUE_NAME, receiptId);
        }
    }

    @Test
    public void pushedItemsAreRetrievedByPull() throws Exception {

        Optional<QueueMessage> msgOpt = service.pull(TEST_QUEUE_NAME);
        receiptId = msgOpt.get().getReceiptId();
        assertTrue(TEST_QUEUE_NAME, msgOpt.isPresent());
        assertEquals(MESSAGE_BODY, msgOpt.get().getMessageBody());
        assertNotNull(msgOpt.get().getReceiptId());
    }
}