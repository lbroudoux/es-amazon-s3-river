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

import java.io.Serializable;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;
/**
 * This is a simple wrapper for carrying picked up summaries of S3 bucket objects
 * that mastches the last modification date criteria along with the keys of any objects
 * regardless their modification date.
 * @author laurent
 */
public class S3ObjectSummaries implements Serializable{

   /** Default serial version UID. */
   private static final long serialVersionUID = 1L;

   private Long lastScanTime;
   
   private List<String> keys;
   private List<S3ObjectSummary> pickedSummaries;

   
   public S3ObjectSummaries(Long lastScanTime, List<S3ObjectSummary> summaries, List<String> keys){
      this.lastScanTime = lastScanTime;
      this.pickedSummaries = summaries;
      this.keys = keys;
   }
   
   public Long getLastScanTime(){
      return lastScanTime;
   }

   public List<String> getKeys(){
      return keys;
   }
   
   public List<S3ObjectSummary> getPickedSummaries(){
      return pickedSummaries;
   }
}
