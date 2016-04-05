package com.example.service;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.example.message.QueueMessage;

public class FileQueueService implements QueueService {

    private static final String BASE_PATH = "sqs";
    private static final String LOCK_FILE = ".lock";
    private static final String MESSAGES_FILE = "messages";

    Map<String, String> msgFileMap = new HashMap<>();

    public FileQueueService() {
        createBaseDirIfNotExists();
    }

    private void createBaseDirIfNotExists() {
        File baseDir = new File(BASE_PATH);
        if (!baseDir.exists()) {
            baseDir.mkdir();
        }
    }

    @Override
    public void push(String queueName, String message) {
        createQueueIfNotExists(queueName);
    }

    @Override
    public Optional<QueueMessage> pull(String queueName) {
        return null;
    }

    @Override
    public void delete(String queueName, String messageReceipt) {

    }

    private void createQueueIfNotExists(String queueName) {
        File queueDir = new File(BASE_PATH + File.separator + queueName);
        if ((!msgFileMap.containsKey(queueName) && !queueDir.exists()) || !queueDir.isDirectory()) {
            if (queueDir.mkdir()) {
                File messagesFile = new File(queueDir.getAbsolutePath() + File.separator + MESSAGES_FILE);
                try {
                    messagesFile.createNewFile();
                    msgFileMap.put(queueName, messagesFile.getAbsolutePath());
                } catch (IOException e) {
                    throw new RuntimeException("Message file creation for queue " + queueName + " has failed");
                }
            } else {
                throw new RuntimeException("Queue directory creation for queue " + queueName + " has failed");
            }
        }
    }
}
