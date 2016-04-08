package com.example.service;

import org.junit.After;
import org.junit.Test;


public class InMemoryQueueServiceTest extends GenericQueueServiceTest {
    public InMemoryQueueServiceTest() {
        super(new InMemoryQueueService());
    }

    @After
    public void tearDown() {
        ((InMemoryQueueService) service).queues.clear();
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

    @Test(expected = RuntimeException.class)
    public void deletingNonExistingMessageThrowsException() throws Exception {
        super.deletingNonExistingMessageThrowsException();
    }
}