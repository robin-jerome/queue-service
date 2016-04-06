package com.example.util;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Optional;
import java.util.stream.Stream;

import com.example.message.ManagedQueueMessage;
import com.google.common.io.Files;

public class MessageSerializationUtils {

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
    public static String toString(Serializable obj) {
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

    public static LineOrder getNewLineOrderForPull(File msgFile) {
        LineOrder lineOrder = null;
        try (BufferedReader br = new BufferedReader(new FileReader(msgFile))) {
            lineOrder = new LineOrder();
            String line;
            while ((line = br.readLine()) != null) {
                lineOrder = getUpdatedLineOrderForPull(lineOrder, line);
            }
        } catch (IOException e) {
            throw new RuntimeException("Exception while pull or delete of message", e);
        } finally {
            return lineOrder;
        }
    }

    private static LineOrder getUpdatedLineOrderForPull(LineOrder lineOrder, String line) {
        if (lineOrder.getModifiedMessageOpt().isPresent()) {
            // Message for pull has been identified
            lineOrder.getAfter().add(line);
        } else {
            ManagedQueueMessage message = (ManagedQueueMessage) MessageSerializationUtils.fromString(line);
            if (message.isConsumable()) {
                message.markAsConsumed();
                lineOrder.setModifiedMessageOpt(Optional.of(message));
            } else {
                lineOrder.getBefore().add(line);
            }
        }
        return lineOrder;
    }

    private static LineOrder getUpdatedLineOrderForDelete(LineOrder lineOrder, String line, String messageReceipt) {
        if (lineOrder.getModifiedMessageOpt().isPresent()) {
            // Message for delete has been identified
            lineOrder.getAfter().add(line);
        } else {
            ManagedQueueMessage message = (ManagedQueueMessage) MessageSerializationUtils.fromString(line);
            if (message.getReceiptId().equals(messageReceipt)) { // Equality of receipt id
                lineOrder.setModifiedMessageOpt(Optional.of(message));
            } else {
                lineOrder.getBefore().add(line);
            }
        }
        return lineOrder;
    }

    public static void recreateQueueFile(File msgFile, LineOrder lineOrder, boolean includeModifiedMessage) {
        Stream combinedStream;
        if (includeModifiedMessage) {
            combinedStream = Stream.concat(lineOrder.getBefore().stream(),
                    Stream.concat(Stream.of(toString(lineOrder.getModifiedMessageOpt().get())), lineOrder.getAfter().stream()));
        } else {
            combinedStream = Stream.concat(lineOrder.getBefore().stream(), lineOrder.getAfter().stream());
        }
        combinedStream.forEach(l -> {
            try {
                Files.append(l + System.lineSeparator(), msgFile, Charset.forName("UTF-8"));
            } catch (IOException e) {
                throw new RuntimeException("Exception while recreating queue file", e);
            }
        });
    }

    public static LineOrder getNewLineOrderForDelete(File msgFile, String messageReceipt) {
        LineOrder lineOrder = null;
        try (BufferedReader br = new BufferedReader(new FileReader(msgFile))) {
            lineOrder = new LineOrder();
            String line;
            while ((line = br.readLine()) != null) {
                lineOrder = getUpdatedLineOrderForDelete(lineOrder, line, messageReceipt);
            }
        } catch (IOException e) {
            throw new RuntimeException("Exception while pull or delete of message", e);
        } finally {
            return lineOrder;
        }
    }
}
