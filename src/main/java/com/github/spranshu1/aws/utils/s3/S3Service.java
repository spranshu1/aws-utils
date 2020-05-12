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
package com.github.spranshu1.aws.utils.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * S3 service that provides s3 client used for managing buckets at that
 * 'endPoint'
 */
public class S3Service {
    private AmazonS3 s3client;
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Service.class);

    /**
     * Instantiates a new S3 service.
     *
     * @param credentials the credentials
     */
    public S3Service(final BasicAWSCredentials credentials) {

        s3client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }

    /**
     * Instantiates a new S 3 service.
     *
     * @param credentials  the credentials
     * @param clientConfig the client config
     */
    public S3Service(final BasicAWSCredentials credentials, final ClientConfiguration clientConfig) {
        s3client = AmazonS3ClientBuilder.standard()
                .withClientConfiguration(clientConfig)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }

    /**
     * Returns the S3 client
     *
     * @return AmazonS3 s 3 client
     */
    public AmazonS3 getS3Client() {
        return s3client;
    }

    /**
     * Creates bucket with 'bucketName' if it does not already exists
     *
     * @param bucketName the bucket name
     */
    public void createBucket(String bucketName) {

        s3client.createBucket(bucketName);

    }

    /**
     * Lists all the buckets present at the endPoint
     *
     * @return list of bucket names
     */
    public List<String> listBuckets() {
        List<String> bucketNames = new ArrayList<>();
        for (Bucket bucket : s3client.listBuckets()) {
            bucketNames.add(bucket.getName());
        }
        return bucketNames;
    }

    /**
     * Delete the bucket with 'bucketName'
     *
     * @param bucketName the bucket name
     * @throws Exception the exception
     */
    public void deleteBucket(String bucketName) throws Exception {
        try {
            emptyBucket(bucketName);
            s3client.deleteBucket(bucketName);
        } catch (Exception e) {
            LOGGER.error("Bucket could not be deleted due to exception {}", e);
            throw e;
        }
    }

    /**
     * Empty the contents of the bucket
     *
     * @param bucketName the bucket name
     * @throws Exception the exception
     */
    public void emptyBucket(String bucketName) throws Exception {
        try {
            ObjectListing objects = s3client.listObjects(bucketName);
            while (true) {
                for (Iterator<?> iterator = objects.getObjectSummaries().iterator(); iterator.hasNext(); ) {
                    S3ObjectSummary objectSummary = (S3ObjectSummary) iterator.next();
                    s3client.deleteObject(bucketName, objectSummary.getKey());
                }

                if (objects.isTruncated()) {
                    objects = s3client.listNextBatchOfObjects(objects);
                } else {
                    break;
                }
            }
            // delete all versions of the bucket
            VersionListing list = s3client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
            for (Iterator<?> iterator = list.getVersionSummaries().iterator(); iterator.hasNext(); ) {
                S3VersionSummary versionSummary = (S3VersionSummary) iterator.next();
                s3client.deleteVersion(bucketName, versionSummary.getKey(), versionSummary.getVersionId());
            }
        } catch (Exception e) {
            LOGGER.error("Bucket could not be emptied because of exception {}", e);
            throw e;
        }
    }
}