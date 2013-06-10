/*
 * Licensed to Laurent Broudoux (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.lbroudoux.elasticsearch.river.s3.connector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
/**
 * This is a connector for querying and retrieving files or folders from
 * an Amazon S3 bucket. Credentials are mandatory for connecting to remote drive.
 * @author laurent
 */
public class S3Connector{

   private static final ESLogger logger = Loggers.getLogger(S3Connector.class);
   
   private final String accessKey;
   private final String secretKey;
   private String bucketName;
   private String pathPrefix;
   private AmazonS3Client s3Client;
   
   public S3Connector(String accessKey, String secretKey){
      this.accessKey = accessKey;
      this.secretKey = secretKey;
   }
   
   /**
    * Connect to the specified bucket using previously given accesskey and secretkey.
    * @param bucketName Name of the bucket to connect to
    * @param pathPrefix Prefix that will be later used for filtering documents
    */
   public void connectUserBucket(String bucketName, String pathPrefix){
      this.bucketName = bucketName;
      this.pathPrefix = pathPrefix;
      AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
      s3Client = new AmazonS3Client(credentials);
      String location = s3Client.getBucketLocation(bucketName);
   }
   
   /**
    * Select and retrieves summaries of object into bucket and of given path prefix
    * that have modification date younger than lastScanTime.
    * @param lastScanTime Last modification date filter
    * @return Summaries of picked objects.
    */
   public S3ObjectSummaries getObjectSummaries(Long lastScanTime){
      if (logger.isDebugEnabled()){
         logger.debug("Getting buckets changes since {}", lastScanTime);
      }
      List<String> keys = new ArrayList<String>();
      List<S3ObjectSummary> result = new ArrayList<S3ObjectSummary>();
      
      // Store the scan time to return before doing big queries...
      Long lastScanTimeToReturn = System.currentTimeMillis();
      if (lastScanTime == null){
         lastScanTime = 0L;
      }
      
      ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName)
            .withPrefix(pathPrefix);
      ObjectListing listing = s3Client.listObjects(request);
      logger.debug("Listing: {}", listing);
      while (!listing.getObjectSummaries().isEmpty() || listing.isTruncated()){
         List<S3ObjectSummary> summaries = listing.getObjectSummaries();
         if (logger.isDebugEnabled()){
            logger.debug("Found {} items in this listObjects page", summaries.size());
         }
         for (S3ObjectSummary summary : summaries){
            if (logger.isDebugEnabled()){
               logger.debug("Getting {} last modified on {}", summary.getKey(), summary.getLastModified());
            }
            keys.add(summary.getKey());
            if (summary.getLastModified().getTime() > lastScanTime){
               logger.debug("  Picked !");
               result.add(summary);
            }
         }
         listing = s3Client.listNextBatchOfObjects(listing);
      }
      
      // Wrap results and latest scan time.
      return new S3ObjectSummaries(lastScanTimeToReturn, result, keys);
   }
   
   /**
    * Download Amazon S3 file as byte array.
    * @param summary The summary of the S3 Object to download
    * @return This file bytes or null if something goes wrong.
    */
   public byte[] getContent(S3ObjectSummary summary){
      if (logger.isDebugEnabled()){
         logger.debug("Downloading file content from {}", summary.getKey());
      }
      // Retrieve object corresponding to key into bucket.
      S3Object object = s3Client.getObject(bucketName, summary.getKey());
      
      InputStream is = null;
      ByteArrayOutputStream bos = null;

      try{
         // Get input stream on S3 Object.
         is = object.getObjectContent();
         bos = new ByteArrayOutputStream();

         byte[] buffer = new byte[4096];
         int len = is.read(buffer);
         while (len > 0) {
            bos.write(buffer, 0, len);
            len = is.read(buffer);
         }

         // Flush and return result.
         bos.flush();
         return bos.toByteArray();
      } catch (IOException e) {
         e.printStackTrace();
         return null;
      } finally {
         if (bos != null){
            try{
               bos.close();
            } catch (IOException e) {
            }
         }
         try{
            is.close();
         } catch (IOException e) {
         }
      }
   }
}
