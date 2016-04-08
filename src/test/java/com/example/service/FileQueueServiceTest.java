package com.example.service;

import org.junit.After;
import org.junit.Test;

public class FileQueueServiceTest extends GenericQueueServiceTest {

    public FileQueueServiceTest() {
        super(new FileQueueService());
    }

    @After
    public void setUp() {
        super.setUp();
        service = new FileQueueService();
    }

    @After
    public void tearDown() {
        super.tearDown();
        receiptQueueNameMap.values().stream().distinct().forEach(queueName -> {
            ((FileQueueService) service).deleteQueue(queueName);
        });
    }

    @Test
    public void pushedItemsAreRetrievedByPull() throws Exception {
        super.pushedItemsAreRetrievedByPull();
    }

    @Test
    public void pushedItemsAreRetrievedInOrder() throws Exception {
        super.pushedItemsAreRetrievedInOrder();
    }

    @Test
    public void pulledItemsBecomeVisibleAfterTimeout() throws Exception {
        super.pulledItemsBecomeVisibleAfterTimeout();
    }

    @Test(expected = RuntimeException.class)
    public void pullFromNonExistingQueueThrowsException() {
        super.pullFromNonExistingQueueThrowsException();
    }

    @Test(expected = RuntimeException.class)
    public void deleteFromNonExistingQueueThrowsException() {
        super.deleteFromNonExistingQueueThrowsException();
    }

    @Test
    public void pushingToNonExistentQueueCreatesQueue() throws Exception {
        super.pushingToNonExistentQueueCreatesQueue();
    }
}
