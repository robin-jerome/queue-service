package com.example.util;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Optional;
import java.util.stream.Stream;

import com.example.message.ManagedQueueMessage;
import com.example.message.QueueMessage;
import com.google.common.io.Files;

public class FileQueueHelper {
    private static final Charset CHARSET = Charset.forName("UTF-8");

    /* Convert Base64 encoded string to object */
    public static Object fromString(String sourceStr) {
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(sourceStr)));
            Object object = ois.readObject();
            return object;
        } catch (Exception e) {
            throw new RuntimeException("Exception while decoding string to object", e);
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException e) {
                    throw new RuntimeException("Exception while closing object stream", e);
                }
            }
        }
    }

    /* Convert object to Base64 encoded string */
    public static String messageToString(Serializable obj) {
        ObjectOutputStream oos = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException("Exception while encoding string to object", e);
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException e) {
                    throw new RuntimeException("Exception while closing object stream", e);
                }
            }
        }
    }

    public static MessageOrder getMessageOrderPostPull(File msgFile) {
        MessageOrder messageOrder = null;
        try (BufferedReader br = new BufferedReader(new FileReader(msgFile))) {
            messageOrder = new MessageOrder();
            String line;
            while ((line = br.readLine()) != null) {
                messageOrder = getUpdatedLineOrderForPull(messageOrder, line);
            }
        } catch (IOException e) {
            throw new RuntimeException("Exception while pull or delete of message", e);
        } finally {
            return messageOrder;
        }
    }

    private static MessageOrder getUpdatedLineOrderForPull(MessageOrder messageOrder, String line) {
        if (messageOrder.getModifiedMessageOpt().isPresent()) {
            // Message for pull has been identified
            messageOrder.getAfter().add(line);
        } else {
            ManagedQueueMessage message = (ManagedQueueMessage) FileQueueHelper.fromString(line);
            if (message.isConsumable()) {
                message.markAsConsumed();
                messageOrder.setModifiedMessageOpt(Optional.of(message));
            } else {
                messageOrder.getBefore().add(line);
            }
        }
        return messageOrder;
    }

    private static MessageOrder getUpdatedLineOrderForDelete(MessageOrder messageOrder, String line, String messageReceipt) {
        if (messageOrder.getModifiedMessageOpt().isPresent()) {
            // Message for delete has been identified
            messageOrder.getAfter().add(line);
        } else {
            ManagedQueueMessage message = (ManagedQueueMessage) FileQueueHelper.fromString(line);
            if (message.getReceiptId().equals(messageReceipt)) { // Equality of receipt id
                messageOrder.setModifiedMessageOpt(Optional.of(message));
            } else {
                messageOrder.getBefore().add(line);
            }
        }
        return messageOrder;
    }

    public static void pushMessagesInQueueFile(File msgFile, MessageOrder messageOrder, boolean includeModifiedMessage) {
        Stream combinedStream;
        if (includeModifiedMessage) {
            combinedStream = Stream.concat(messageOrder.getBefore().stream(),
                    Stream.concat(Stream.of(messageToString(messageOrder.getModifiedMessageOpt().get())), messageOrder.getAfter().stream()));
        } else {
            combinedStream = Stream.concat(messageOrder.getBefore().stream(), messageOrder.getAfter().stream());
        }
        combinedStream.forEach(l -> {
            try {
                Files.append(l + System.lineSeparator(), msgFile, Charset.forName("UTF-8"));
            } catch (IOException e) {
                throw new RuntimeException("Exception while recreating queue file", e);
            }
        });
    }

    public static MessageOrder getNewLineOrderForDelete(File msgFile, String messageReceipt) {
        MessageOrder messageOrder = null;
        try (BufferedReader br = new BufferedReader(new FileReader(msgFile))) {
            messageOrder = new MessageOrder();
            String line;
            while ((line = br.readLine()) != null) {
                messageOrder = getUpdatedLineOrderForDelete(messageOrder, line, messageReceipt);
            }
        } catch (IOException e) {
            throw new RuntimeException("Exception while pull or delete of message", e);
        } finally {
            return messageOrder;
        }
    }

    public static void pushToFileQueue(QueueMessage queueMessage, File queueFile) throws IOException {
        String messageString = messageToString(queueMessage);
        Files.append(messageString + System.lineSeparator(), queueFile, CHARSET);
    }
}
