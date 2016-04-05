package com.example.service;

import org.junit.Test;

public class FileQueueServiceTest {

    private QueueService service = new FileQueueService();
    private static final String TEST_QUEUE_NAME = "myTestQueue";
    private static final String MESSAGE_BODY = "testMsg";

    @Test
    public void pushedItemsAreRetrievedByPull() throws Exception {
        service.push(TEST_QUEUE_NAME, MESSAGE_BODY);
    }

}
