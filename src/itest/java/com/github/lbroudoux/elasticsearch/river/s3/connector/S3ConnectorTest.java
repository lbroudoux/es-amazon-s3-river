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

import com.amazonaws.services.s3.model.AmazonS3Exception;

import org.junit.Test;
/**
 * @author laurent
 */
public class S3ConnectorTest{

   @Test(expected = AmazonS3Exception.class)
   public void shouldNotConnectUserBucketWithBadSecretKey() {
      S3Connector connector = new S3Connector("AKIAITHNRLFUUVPFBKZQ", "azerty");
      connector.connectUserBucket("famillebroudoux", "papiers/");
   }

   @Test(expected = AmazonS3Exception.class)
   public void shouldNotConnectUserBucketWithBadBucket() {
      S3Connector connector = new S3Connector("AKIAITHNRLFUUVPFBKZQ", "<replace by secret>");
      connector.connectUserBucket("azerty", "papiers/");
   }
}
