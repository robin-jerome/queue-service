package com.example.service;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.util.*;

import com.example.message.QueueMessage;
import com.example.message.QueueMessageFactory;
import com.example.util.LineOrder;
import com.example.util.MessageSerializationUtils;
import com.google.common.io.Files;

public class FileQueueService implements QueueService {

    private static final String BASE_PATH = "sqs";
    private static final String LOCK_FILE_NAME = ".lock";
    private static final String MESSAGES_FILE_NAME = "messages";
    private static Map<String, FileLock> queueLocks = new HashMap<>();
    private static final long LOCK_RETRY_DURATION_MILLIS = 250L;
    private static final long MAX_LOCK_WAIT_MILLIS = 3000L;

    // To prevent repetitive file checks
    Set<String> availableQueues = new HashSet<>();

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
        try {
            if (acquireQueueLock(queueName, MAX_LOCK_WAIT_MILLIS)) {
                System.out.println("Lock acquired to push to queue " + queueName);
                QueueMessage queueMessage = QueueMessageFactory.createManagedQueueMessage(message);
                appendMessageToQueue(queueName, queueMessage);
            } else {
                System.err.println("Lock to push to queue " + queueName + " was not acquired ");
            }
        } catch (IOException e) {
            System.out.println("Error while writing " + message + " to queue " + queueName);
        } finally {
            releaseQueueLock(queueName);
        }
    }

    private void appendMessageToQueue(String queueName, QueueMessage queueMessage) throws IOException {
        String messageString = MessageSerializationUtils.toString(queueMessage);
        Files.append(messageString + System.lineSeparator(), new File(toQueueMessagesFilePath(queueName)), Charset.forName("UTF-8"));
    }

    @Override
    public Optional<QueueMessage> pull(String queueName) {
        File msgFile = checkIfQueueExists(queueName);
        try {
            if (acquireQueueLock(queueName, MAX_LOCK_WAIT_MILLIS)) {
                System.out.println("Lock acquired to pull from queue " + queueName);
                LineOrder lineOrder = MessageSerializationUtils.getNewLineOrderForPull(msgFile);
                if (lineOrder.isModified()) {
                    // Pull has a message - return the superclass
                    msgFile.delete();
                    msgFile = new File(toQueueMessagesFilePath(queueName));
                    msgFile.createNewFile();
                    MessageSerializationUtils.recreateQueueFile(msgFile, lineOrder, true); // Include modified element
                    // Update the file with new lines and delete oldLines
                    return Optional.of(lineOrder.getModifiedMessageOpt().get().copy());
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

    private File checkIfQueueExists(String queueName) {
        if (!new File(toQueueMessagesFilePath(queueName)).exists()) {
            throw new RuntimeException("Queue with name " + queueName + " does not exist");
        } else {
            return new File(toQueueMessagesFilePath(queueName));
        }
    }

    @Override
    public void delete(String queueName, String messageReceipt) {
        File msgFile = checkIfQueueExists(queueName);
        try {
            if (acquireQueueLock(queueName, MAX_LOCK_WAIT_MILLIS)) {
                System.out.println("Lock acquired to delete from queue " + queueName);
                LineOrder lineOrder = MessageSerializationUtils.getNewLineOrderForDelete(msgFile, messageReceipt);
                if (lineOrder.isModified()) {
                    msgFile.delete();
                    msgFile = new File(toQueueMessagesFilePath(queueName));
                    msgFile.createNewFile();
                    MessageSerializationUtils.recreateQueueFile(msgFile, lineOrder, false); // Not include modified element
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

    private static boolean acquireQueueLock(String queueName, final long maxWaitTime) {
        FileLock fileLock = queueLocks.get(queueName);
        if (fileLock == null && maxWaitTime > 0) {
            try {
                long dropDeadTime = System.currentTimeMillis() + maxWaitTime;
                File file = new File(toQueueLockFilePath(queueName));
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                FileChannel fileChannel = randomAccessFile.getChannel();
                while (System.currentTimeMillis() < dropDeadTime) {
                    fileLock = fileChannel.tryLock();
                    if (fileLock != null) {
                        break;
                    }
                    Thread.sleep(LOCK_RETRY_DURATION_MILLIS); // 4 attempts/sec
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

    private void createQueueIfNotExists(String queueName) {
        File queueDir = new File(toQueueDirPath(queueName));
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

    private static String toQueueDirPath(String queueName) {
        return BASE_PATH + File.separator + queueName;
    }

    private static String toQueueMessagesFilePath(String queueName) {
        return toQueueDirPath(queueName) + File.separator + MESSAGES_FILE_NAME;
    }

    private static String toQueueLockFilePath(String queueName) {
        return toQueueDirPath(queueName) + File.separator + LOCK_FILE_NAME;
    }
}
