package com.example.service;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.*;


public class InMemoryQueueServiceTest {
    private QueueService service = new InMemoryQueueService();
    private static final String TEST_QUEUE_NAME = "myTestQueue";

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {

    }

    @Test
    public void pushedItemsAreRetrievedByPull() throws Exception {
        String messageBody = "testMsg";
        service.push(TEST_QUEUE_NAME, messageBody);
        assertTrue(TEST_QUEUE_NAME, service.pull(TEST_QUEUE_NAME).isPresent());
        assertEquals(messageBody, service.pull(TEST_QUEUE_NAME).get().getMessageBody());
        assertNotNull(service.pull(TEST_QUEUE_NAME).get().getReceiptId());
    }
}