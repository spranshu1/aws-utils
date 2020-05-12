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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Establish a connection with Amazon SQS endPoint using Amazon AWS credentials
 */
public class SqsQueueService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqsQueueService.class);

    private AmazonSQS sqs;

    /**
     * Instantiates a new Sqs queue service.
     *
     * @param credentials the credentials
     */
    public SqsQueueService(final BasicAWSCredentials credentials) {
        this.sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }

    /**
     * Create a new queue with 'queueName'
     *
     * @param queueName the queue name
     * @return URL of the queue created
     */
    public String createQueue(final String queueName) {
        String queueUrl = getQueueURL(queueName);
        if (queueUrl == null)
            return sqs.createQueue(new CreateQueueRequest(queueName)).getQueueUrl();
        else
            return queueUrl;
    }

    /**
     * Get the URL for the 'queueName' to send or receive messages
     *
     * @param queueName the queue name
     * @return queue URL if queue exists, null otherwise
     */
    public String getQueueURL(String queueName) {
        String queueUrl;
        GetQueueUrlRequest getUrlRequest = new GetQueueUrlRequest();
        getUrlRequest.setQueueName(queueName);
        try {
            GetQueueUrlResult response = sqs.getQueueUrl(getUrlRequest);
            queueUrl = response.getQueueUrl();
        } catch (QueueDoesNotExistException ex) {
            LOGGER.error("Queue does not exist with name : {}",queueName);
            queueUrl = null;
        }
        return queueUrl;
    }

    /**
     * Get URLs of all the queues at this endpoint
     *
     * @return List of URLs of all the queues
     */
    public List<String> getQueueList() {
        return sqs.listQueues().getQueueUrls();
    }


    /**
     * Returns the Amazon SQS client
     *
     * @return sqs sqs
     */
    public AmazonSQS getSqs() {
        return sqs;
    }
}