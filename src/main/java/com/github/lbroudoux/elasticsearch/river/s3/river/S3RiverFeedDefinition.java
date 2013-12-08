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
package com.github.lbroudoux.elasticsearch.river.s3.river;

import java.util.List;
/**
 * A definition bean wrapping information of river feed settings.
 * @author laurent
 */
public class S3RiverFeedDefinition{

   private String feedname;
   private String bucket;
   private String pathPrefix;
   private String downloadHost;
   private int updateRate;
   private List<String> includes;
   private List<String> excludes;
   private String accessKey;
   private String secretKey;
   
   public S3RiverFeedDefinition(String feedname, String bucket, String pathPrefix, String downloadHost, int updateRate, 
         List<String> includes, List<String> excludes, String accessKey, String secretKey){
      this.feedname = feedname;
      this.bucket = bucket;
      this.pathPrefix = pathPrefix;
      this.downloadHost = downloadHost;
      this.updateRate = updateRate;
      this.includes = includes;
      this.excludes = excludes;
      this.accessKey = accessKey;
      this.secretKey = secretKey;
   }
   
   public String getFeedname() {
      return feedname;
   }
   public void setFeedname(String feedname) {
      this.feedname = feedname;
   }
   
   public String getBucket() {
      return bucket;
   }
   public void setBucket(String bucket) {
      this.bucket = bucket;
   }

   public String getPathPrefix() {
      return pathPrefix;
   }
   public void setPathPrefix(String pathPrefix) {
      this.pathPrefix = pathPrefix;
   }
   
   public String getDownloadHost() {
      return downloadHost;
   }
   public void setDownloadHost(String downloadHost) {
      this.downloadHost = downloadHost;
   }

   public int getUpdateRate() {
      return updateRate;
   }
   public void setUpdateRate(int updateRate) {
      this.updateRate = updateRate;
   }
   
   public List<String> getIncludes() {
      return includes;
   }
   public void setIncludes(List<String> includes) {
      this.includes = includes;
   }

   public List<String> getExcludes() {
      return excludes;
   }
   public void setExcludes(List<String> excludes) {
      this.excludes = excludes;
   }

   public String getAccessKey() {
      return accessKey;
   }
   public void setAccessKey(String accessKey) {
      this.accessKey = accessKey;
   }

   public String getSecretKey() {
      return secretKey;
   }
   public void setSecretKey(String secretKey) {
      this.secretKey = secretKey;
   }
}
