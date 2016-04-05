package com.example.service;

import java.util.Optional;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.example.message.QueueMessage;
import com.example.message.QueueMessageFactory;

public class SQSQueueService implements QueueService {
    private AmazonSQSClient sqsClient;
    // This property should come from flavours
    private static final String QUEUE_END_POINT = "https://sqs.eu-west-1.amazonaws.com";
    // This property should come from flavours
    private static final String ACCOUNT_NUMBER = "123456789012";

    public SQSQueueService(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    @Override
    public void push(String queueName, String messageBody) {
        sqsClient.sendMessage(toSQSQueueUrl(queueName), messageBody);
    }

    @Override
    public Optional<QueueMessage> pull(String queueName) {
        // By default only one message is received from SQS client
        return Optional.ofNullable(toQueueMessage(sqsClient.receiveMessage(toSQSQueueUrl(queueName))));
    }

    @Override
    public void delete(String queueName, String messageReceipt) {
        sqsClient.deleteMessage(toSQSQueueUrl(queueName), messageReceipt);
    }

    private QueueMessage toQueueMessage(ReceiveMessageResult receiveMessageResult) {
        if (receiveMessageResult.getMessages().isEmpty()) {
            return null;
        } else {
            Message message = receiveMessageResult.getMessages().get(0);
            return QueueMessageFactory.convertToQueueMessage(message.getBody(), message.getReceiptHandle());
        }
    }

    /**
     *  Documentation http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/ImportantIdentifiers.html
     *  or some other alternative mechanism
     */
    private String toSQSQueueUrl(String queueName) {
        return QUEUE_END_POINT + "/" + ACCOUNT_NUMBER + "/" + queueName;
    }
}
