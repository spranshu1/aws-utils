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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Instance for operating on one bucket with a particular bucketName
 */
public class S3BucketHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3BucketHelper.class);


    private String bucketName;
    private TransferManager transferManager;
    private AmazonS3 s3client;

    /**
     * Instantiates a new S3 bucket.
     *
     * @param bucketName the bucket name
     * @param s3client   the s3 client
     */
    public S3BucketHelper(String bucketName, AmazonS3 s3client) {
        this.bucketName = bucketName;
        this.s3client = s3client;
        transferManager = TransferManagerBuilder.standard()
                        .withS3Client(s3client)
                        .build();
    }

    /**
     * Instantiates a new S3 bucket.
     *
     * @param bucketName     the bucket name
     * @param s3client       the s3 client
     * @param threadPoolSize the thread pool size
     */
    public S3BucketHelper(final String bucketName, final AmazonS3 s3client, final int threadPoolSize) {
        this.bucketName = bucketName;
        this.s3client = s3client;
        transferManager = TransferManagerBuilder.standard()
                        .withS3Client(s3client)
                        .withExecutorFactory(() -> Executors.newFixedThreadPool(threadPoolSize))
                        .build();
    }

    /**
     * Sets the region for amazon s3 bucket
     *
     * @param region the region
     */
    public void setRegion(final Regions region) {
        s3client.setRegion(Region.getRegion(region));
    }

    /**
     * Upload any object to the 'bucketName' through input stream
     *
     * @param objectStream the object stream
     * @param key          the key
     * @throws Exception the exception
     */
    public void uploadObject(InputStream objectStream, String key) throws Exception {
        try {
            Upload uploadObject = transferManager.upload(bucketName, key, objectStream, new ObjectMetadata());
            uploadObject.waitForCompletion();
        } catch (Exception ex) {
            LOGGER.error("Uploading of object could not be completed due to exception {}", ex.getMessage());
            throw ex;
        }
    }

    /**
     * Upload any object to the 'bucketName' through input stream
     *
     * @param objectStream  the object stream
     * @param contentLength the content length
     * @param key           the key
     * @throws Exception the exception
     */
    public void uploadObject(InputStream objectStream, long contentLength, String key) throws Exception {
        try {
            ObjectMetadata metaData = new ObjectMetadata();
            metaData.setContentLength(contentLength);
            Upload uploadObject = transferManager.upload(bucketName, key, objectStream, metaData);
            uploadObject.waitForCompletion();
        } catch (Exception e) {
            LOGGER.error("Uploading of object could not be completed due to exception {}", e.getMessage());
            throw e;
        }
    }

    /**
     * List all objects with keys that have a particular prefix
     *
     * @param prefix the prefix
     * @return list of matching keys
     * @throws Exception the exception
     */
    public List<String> listObjects(String prefix) throws Exception {
        List<String> matchingKeys = new ArrayList<>();
        try {
            ObjectListing objects = s3client.listObjects(bucketName, prefix);
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                matchingKeys.add(objectSummary.getKey());
            }
        } catch (Exception e) {
            LOGGER.error("Objects could not be listed due to exception {}", e.getMessage());
            throw e;
        }
        return matchingKeys;
    }

    /**
     * Checks whether the input key exists in the instance bucket. Returns true
     * if found, false if 404 from client or throws exception otherwise.
     *
     * @param key the key
     * @return key exists
     */
    public boolean checkKeyExists(String key) {
        try {
            s3client.getObjectMetadata(bucketName, key);
            return true;
        } catch (AmazonServiceException e) {
            LOGGER.error("Objects could not be listed due to exception {}", e.getMessage());
            if (404 == e.getStatusCode())
                return false;
            throw e;
        }
    }

    /**
     * Download any object from the 'bucketName'
     *
     * @param key the key
     * @return S3ObjectInputStream s 3 object input stream
     * @throws Exception the exception
     */
    public S3ObjectInputStream downloadObject(String key) throws Exception {
        try {
            S3Object downloadedObj = s3client.getObject(bucketName, key);
            return downloadedObj.getObjectContent();
        } catch (Exception e) {
            LOGGER.error("Downloading of object could not be completed due to exception {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Download any object into a file from the 'bucketName'
     *
     * @param file the file
     * @param key  the key
     * @throws Exception the exception
     */
    public void downloadFile(File file, String key) throws Exception {
        Download downloadedObj = transferManager.download(bucketName, key, file);
        try {
            downloadedObj.waitForCompletion();
        } catch (Exception e) {
            LOGGER.error("Downloading of file could not be completed due to exception {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Upload a file to 'bucketName'
     *
     * @param file        the file
     * @param key         the key
     * @param asyncUpload the async upload
     * @throws Exception the exception
     */
    public void uploadFile(File file, String key, boolean asyncUpload) throws Exception {
        try {
            Upload uploadObject = transferManager.upload(bucketName, key, file);
            if (!asyncUpload) {
                uploadObject.waitForCompletion();
            }
        } catch (Exception e) {
            LOGGER.error("Uploading of object could not be completed due to exception {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Delete an object from 'bucketName'
     *
     * @param key the key
     * @throws Exception the exception
     */
    public void deleteObject(String key) throws Exception {
        try {
            s3client.deleteObject(bucketName, key);
        } catch (Exception e) {
            LOGGER.error("Object could not be deleted due to the exception {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Delete multiple objects
     *
     * @param keys the keys
     * @throws Exception the exception
     */
    public void deleteObjects(List<String> keys) throws Exception {
        List<DeleteObjectsRequest.KeyVersion> Keys = new ArrayList<>();

        for (String key : keys)
            Keys.add(new DeleteObjectsRequest.KeyVersion(key));

        DeleteObjectsRequest delObjects = new DeleteObjectsRequest(bucketName);
        delObjects.setKeys(Keys);
        try {
            s3client.deleteObjects(delObjects);
        } catch (Exception e) {
            LOGGER.error("Some objects could not be deleted due to the exception {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Close.
     */
    public void close() {
        transferManager.shutdownNow();
    }
}