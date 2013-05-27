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

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
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
    * 
    * @param bucketName
    * @param pathPrefix
    */
   public void connectUserBucket(String bucketName, String pathPrefix){
      this.bucketName = bucketName;
      this.pathPrefix = pathPrefix;
      AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
      s3Client = new AmazonS3Client(credentials);
      String location = s3Client.getBucketLocation(bucketName);
   }
   
   /**
    * 
    * @param lastScanTime
    * @return
    */
   public S3Changes getChanges(Long lastScanTime){
      if (logger.isDebugEnabled()){
         logger.debug("Getting buckets changes since {}", lastScanTime);
      }
      List<S3ObjectSummary> result = new ArrayList<S3ObjectSummary>();
      
      if (lastScanTime == null){
         lastScanTime = System.currentTimeMillis();
      }
      
      ObjectListing listing = s3Client.listObjects(bucketName, pathPrefix);
      while (listing.isTruncated()){
         List<S3ObjectSummary> summaries = listing.getObjectSummaries();
         for (S3ObjectSummary summary : summaries){
            if (summary.getLastModified().getTime() > lastScanTime){
               result.add(summary);
            }
         }
         listing = s3Client.listNextBatchOfObjects(listing);
      }
      
      // Wrap results and latest scan time.
      return new S3Changes(lastScanTime, result);
   }
}
