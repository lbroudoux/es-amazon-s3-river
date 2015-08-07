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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.model.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.github.lbroudoux.elasticsearch.river.s3.river.S3RiverFeedDefinition;
/**
 * This is a connector for querying and retrieving files or folders from
 * an Amazon S3 bucket. Credentials are mandatory for connecting to remote drive.
 * @author laurent
 */
public class S3Connector{

   private static final ESLogger logger = Loggers.getLogger(S3Connector.class);
   
   private final String accessKey;
   private final String secretKey;
   private boolean useIAMRoleForEC2 = false;
   private String bucketName;
   private String pathPrefix;
   private AmazonS3Client s3Client;

   /**
    * Create a S3Connector with security credentials. This is helpful if you want
    * to use IAM Roles as described here http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-roles.html.
    */
   public S3Connector(boolean useIAMRoleForEC2) {
      this.accessKey = null;
      this.secretKey = null;
      this.useIAMRoleForEC2 = useIAMRoleForEC2;
   }

   /**
    * Create a SEConnector with provided security credentials.
    * @param accessKey The AWS access key such as provided by AWS console
    * @param secretKey The AWS secret key such as provided by AWS console
    */
   public S3Connector(String accessKey, String secretKey){
      this.accessKey = accessKey;
      this.secretKey = secretKey;
   }
   
   /**
    * Connect to the specified bucket using previously given accesskey and secretkey.
    * @param bucketName Name of the bucket to connect to
    * @param pathPrefix Prefix that will be later used for filtering documents
    * @throws AmazonS3Exception when access or secret keys are wrong or bucket does not exists
    */
   public void connectUserBucket(String bucketName, String pathPrefix) throws AmazonS3Exception{
      this.bucketName = bucketName;
      this.pathPrefix = pathPrefix;
      if (accessKey != null && secretKey != null) {
         AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
         s3Client = new AmazonS3Client(credentials);
      } else if (useIAMRoleForEC2) {
         // Force usage of IAM Role process as described into
         // http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-roles.html.
         s3Client = new AmazonS3Client(new InstanceProfileCredentialsProvider());
      } else {
         // Default credentials retrieval or IAM Role process as described into
         // http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-roles.html.
         s3Client = new AmazonS3Client();
      }
      // Getting location seems odd as we don't use it later and doesBucketExists() seems
      // more appropriate... However, this later returns true even for non existing buckets !
      s3Client.getBucketLocation(bucketName);
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
   
   public Map<String,Object> getS3UserMetadata(String key){ 
	   return Collections.<String, Object>unmodifiableMap(s3Client.getObjectMetadata(bucketName, key).getUserMetadata());
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
   
   /**
    * Get the download url of this S3 object. May return null if the
    * object bucket and key cannot be converted to a URL.
    * @param summary A S3 object
    * @param feedDefinition The holder of S3 feed definition.
    * @return The resource url if possible (access is subject to AWS credential)
    */
   public String getDownloadUrl(S3ObjectSummary summary, S3RiverFeedDefinition feedDefinition){
      String resourceUrl = s3Client.getResourceUrl(summary.getBucketName(), summary.getKey()); 
      // If a download host (actually a vhost such as cloudfront offers) is specified, use it to
      // recreate a vhosted resource url. This is made by substitution of the generic host name in url. 
      if (resourceUrl != null && feedDefinition.getDownloadHost() != null){
         int hostPosEnd = resourceUrl.indexOf("s3.amazonaws.com/") + "s3.amazonaws.com".length();
         String vhostResourceUrl = feedDefinition.getDownloadHost() + resourceUrl.substring(hostPosEnd);
         return vhostResourceUrl;
      }
      return resourceUrl;
   }
}
