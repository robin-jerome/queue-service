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

}
