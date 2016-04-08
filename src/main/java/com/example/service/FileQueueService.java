package com.example.service;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;

import com.example.message.QueueMessage;
import com.example.message.QueueMessageFactory;
import com.example.util.FileQueueHelper;
import com.example.util.MessageOrder;

public class FileQueueService implements QueueService {
    private static final String BASE_DIRECTORY = "sqs";
    private static final String LOCK_FILE_NAME = ".lock";
    private static final String MESSAGES_FILE_NAME = "messages";
    private static final long LOCK_RETRY_DURATION_MILLIS = 250L;
    private static final long MAX_LOCK_WAIT_MILLIS = 3000L;
    private static Map<String, FileLock> queueLocks = new HashMap<>();

    // To prevent repetitive checks for existence of queue by name
    Set<String> availableQueues = new HashSet<>();

    public FileQueueService() {
        createBaseDirectoryIfNotExists();
    }

    @Override
    public void push(String queueName, String message) {
        createQueueIfNotExists(queueName);
        try {
            if (acquireQueueLock(queueName, MAX_LOCK_WAIT_MILLIS)) {
                System.out.println("Lock acquired to push to queue " + queueName);
                pushToFileQueue(queueName, QueueMessageFactory.createManagedQueueMessage(message));
            } else {
                System.err.println("Lock to push to queue " + queueName + " was not acquired ");
            }
        } catch (IOException e) {
            System.out.println("Error while writing " + message + " to queue " + queueName);
        } finally {
            releaseQueueLock(queueName);
        }
    }

    @Override
    public Optional<QueueMessage> pull(String queueName) {
        File msgFile = checkIfQueueExists(queueName);
        try {
            if (acquireQueueLock(queueName, MAX_LOCK_WAIT_MILLIS)) {
                System.out.println("Lock acquired to pull from queue " + queueName);
                MessageOrder messageOrder = FileQueueHelper.getMessageOrderPostPull(msgFile);
                if (messageOrder.isModified()) {
                    msgFile = recreateMessageFileForQueue(queueName, msgFile);
                    FileQueueHelper.pushMessagesInQueueFile(msgFile, messageOrder, true); // Include modified message
                    // Return fetched message
                    return Optional.of(messageOrder.getModifiedMessageOpt().get().copy());
                }
            } else {
                System.err.println("Lock to pull to queue " + queueName + " was not acquired ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            releaseQueueLock(queueName);
        }
        return Optional.empty();
    }

    @Override
    public void delete(String queueName, String messageReceipt) {
        File msgFile = checkIfQueueExists(queueName);
        try {
            if (acquireQueueLock(queueName, MAX_LOCK_WAIT_MILLIS)) {
                System.out.println("Lock acquired to delete from queue " + queueName);
                MessageOrder messageOrder = FileQueueHelper.getNewLineOrderForDelete(msgFile, messageReceipt);
                if (messageOrder.isModified()) {
                    msgFile = recreateMessageFileForQueue(queueName, msgFile);
                    FileQueueHelper.pushMessagesInQueueFile(msgFile, messageOrder, false); // Not include modified element
                }
            } else {
                System.err.println("Lock to delete from queue " + queueName + " was not acquired ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            releaseQueueLock(queueName);
        }

    }

    private void createBaseDirectoryIfNotExists() {
        File baseDir = new File(BASE_DIRECTORY);
        if (!baseDir.exists()) {
            baseDir.mkdir();
        }
    }

    private File checkIfQueueExists(String queueName) {
        if (!new File(toQueueMessagesFilePath(queueName)).exists()) {
            throw new RuntimeException("Queue with name " + queueName + " does not exist");
        } else {
            return new File(toQueueMessagesFilePath(queueName));
        }
    }

    private void createQueueIfNotExists(String queueName) {
        File queueDir = new File(toQueueDirectoryPath(queueName));
        if ((!availableQueues.contains(queueName) && !queueDir.exists()) || !queueDir.isDirectory()) {
            if (queueDir.mkdir()) {
                File messagesFile = new File(toQueueMessagesFilePath(queueName));
                try {
                    messagesFile.createNewFile();
                    availableQueues.add(queueName);
                    queueLocks.put(queueName, null);
                } catch (IOException e) {
                    throw new RuntimeException("Message file creation for queue " + queueName + " has failed");
                }
            } else {
                throw new RuntimeException("Queue directory creation for queue " + queueName + " has failed");
            }
        }
    }

    private File recreateMessageFileForQueue(String queueName, File msgFile) throws IOException {
        msgFile.delete();
        msgFile = new File(toQueueMessagesFilePath(queueName));
        msgFile.createNewFile();
        return msgFile;
    }

    private static boolean acquireQueueLock(String queueName, final long maxWaitTime) {
        FileLock fileLock = queueLocks.get(queueName);
        if (fileLock == null && maxWaitTime > 0) {
            try {
                long terminationTime = System.currentTimeMillis() + maxWaitTime;
                File file = new File(toQueueLockFilePath(queueName));
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                FileChannel fileChannel = randomAccessFile.getChannel();
                while (System.currentTimeMillis() < terminationTime) {
                    fileLock = fileChannel.tryLock();
                    if (fileLock != null) {
                        break;
                    }
                    Thread.sleep(LOCK_RETRY_DURATION_MILLIS); // 4 attempts in a sec
                }
            } catch (Exception e) {
                throw new RuntimeException("Error occured while acquiring lock for queue " + queueName, e);
            }
        }
        queueLocks.put(queueName, fileLock);
        return fileLock == null ? false : true;
    }

    private static void releaseQueueLock(String queueName) {
        FileLock fileLock = queueLocks.get(queueName);
        if (fileLock != null) {
            try {
                fileLock.release();
                queueLocks.put(queueName, null);
            } catch (IOException e) {
                throw new RuntimeException("Error occured while releasing lock for queue " + queueName, e);
            }
        }
    }

    private void pushToFileQueue(String queueName, QueueMessage queueMessage) throws IOException {
        FileQueueHelper.pushToFileQueue(queueMessage, new File(toQueueMessagesFilePath(queueName)));
    }

    private static String toQueueDirectoryPath(String queueName) {
        return BASE_DIRECTORY + File.separator + queueName;
    }

    private static String toQueueMessagesFilePath(String queueName) {
        return toQueueDirectoryPath(queueName) + File.separator + MESSAGES_FILE_NAME;
    }

    private static String toQueueLockFilePath(String queueName) {
        return toQueueDirectoryPath(queueName) + File.separator + LOCK_FILE_NAME;
    }

    void deleteQueue(String queueName) {
        FileQueueHelper.deleteRecursively(new File(toQueueDirectoryPath(queueName)));
    }
}
