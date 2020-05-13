/*
 * Created By: Pranshu Shrivastava

 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.spranshu1.aws.utils.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Send Message to a particular queue with 'queueName' or Receive Message from 'queueName'
 */
public class SqsQueue {
	private AmazonSQS sqs;
	private String queueUrl;
	private int maxMessages;
	private ReceiveMessageRequest receiveReq;

	/**
	 * Instantiates a new Sqs queue.
	 *
	 * @param queueUrl the queue url
	 * @param sqs      the sqs
	 */
	public SqsQueue(final String queueUrl, final AmazonSQS sqs) {
		this.sqs = sqs;
		this.queueUrl = queueUrl;
		this.maxMessages = 10;
	}

	/**
	 * Set the number of Maximum number of messages received at a time from the
	 * queue
	 * NOTE : max number of messages cannot be more than 10
	 * @param n the number of messages
	 */
	public void setMaxNumberOfMessage(final int n) {
		maxMessages = (n > 10) ? 10 : n; // max number of messages cannot be more than 10
	}

	/**
	 * Receive a maxNumOfMsg from the queue.
	 * Use for short polling
	 *
	 * @return list of messages received
	 */
	public List<Message> receiveMessage() {
		receiveReq = new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(maxMessages);
		return sqs.receiveMessage(receiveReq).getMessages();
	}

	/**
	 * Receive a maxNumOfMsg from the queue (wait for 'waitTimeSeconds' before returning if no message in queue).
	 * Use for long polling
	 *
	 * @param waitTimeSeconds the wait time seconds
	 * @return list of messages received
	 */
	public List<Message> receiveMessage(final int waitTimeSeconds) {
		receiveReq = new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(maxMessages).withWaitTimeSeconds(waitTimeSeconds);
		return sqs.receiveMessage(receiveReq).getMessages();
	}

	/**
	 * Delete a message from the queue
	 *
	 * @param message to be deleted
	 */
	public void deleteMessage(final Message message) {
		sqs.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
	}

	/**
	 * Delete a message from the queue, given receiptHandle for the message
	 *
	 * @param receiptHandle of the message to be deleted
	 */
	public void deleteMessage(final String receiptHandle) {
		sqs.deleteMessage(new DeleteMessageRequest(queueUrl, receiptHandle));
	}

	/**
	 * Send a message to the queue
	 *
	 * @param message the message
	 * @return message id
	 */
	public String sendMessage(final String message) {
		return sqs.sendMessage(new SendMessageRequest(queueUrl, message)).getMessageId();
	}

	/**
	 * Send messages in bulk
	 *
	 * @param messages the messages
	 * @return list of failure messages
	 */
	public List<BatchResultErrorEntry> sendMessageBulk(List<String> messages) {
		List<SendMessageBatchRequestEntry> entries;
		List<BatchResultErrorEntry> result = new ArrayList<>();
		for (Integer batch = 0; batch < messages.size(); batch += 10) {
			entries = new ArrayList<>();
			for (Integer id = 0; id < 10 && (batch + id) < messages.size(); id++) {
				entries.add(new SendMessageBatchRequestEntry(id.toString(), messages.get(batch + id)));
			}
			result.addAll(sqs.sendMessageBatch(queueUrl, entries).getFailed());
		}
		return result;
	}

	/**
	 * Changes visibility timeout for a specific message
	 *
	 * @param message           the message
	 * @param visibilityTimeout the visibility timeout
	 */
	public void changeMessageVisibilityTimeOut(final Message message, final int visibilityTimeout){
		sqs.changeMessageVisibility(queueUrl, message.getReceiptHandle(), visibilityTimeout);
	}

	/**
	 * Send a message to the queue with a delay
	 *
	 * @param message      the message
	 * @param delaySeconds the delay seconds
	 * @return messageId string
	 */
	public String sendMessageWithDelay(final String message, final Integer delaySeconds) {
		return sqs.sendMessage(new SendMessageRequest(queueUrl, message).withDelaySeconds(delaySeconds)).getMessageId();
	}

	/**
	 * Gets queue url.
	 *
	 * @return the queue url
	 */
	public String getQueueURL() {
		return queueUrl;
	}

	/**
	 * Delete the Queue
	 */
	public void deleteQueue() {
		sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
	}

	/**
	 * Delete all the messages from the queue (takes 60 sec to purge)
	 */
	public void deleteAllMessages() {
		sqs.purgeQueue(new PurgeQueueRequest(queueUrl));
	}

	/**
	 * Set attributes map in the form <Attribute Name, Attribute Value>
	 *
	 * @param attributes the attributes
	 */
	public void setQueueAttributes(Map<String, String> attributes) {
		sqs.setQueueAttributes(queueUrl, attributes);
	}

	/**
	 * Shuts down the connection to Amazon SQS
	 */
	public void shutdown(){
		sqs.shutdown();
	}

}