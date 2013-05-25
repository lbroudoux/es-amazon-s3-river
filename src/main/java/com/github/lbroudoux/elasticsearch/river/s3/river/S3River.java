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

import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
/**
 * 
 * @author laurent
 */
public class S3River extends AbstractRiverComponent implements River{

   private final Client client;
   
   private final String indexName;

   private final String typeName;

   private final long bulkSize;
   
   private volatile Thread feedThread;
   
   private volatile boolean closed = false;
   
   
   @Inject
   @SuppressWarnings({ "unchecked" })
   protected S3River(RiverName riverName, RiverSettings settings, Client client) throws Exception{
      super(riverName, settings);
      this.client = client;
      
      // Deal with connector settings.
      if (settings.settings().containsKey("amazon-s3")){
         Map<String, Object> feed = (Map<String, Object>)settings.settings().get("amazon-s3");
         
         // Retrieve feed settings.
         
      } else {
         logger.error("You didn't define the amazon-s3 settings. Exiting... See https://github.com/lbroudoux/es-amazon-s3-river");
         indexName = null;
         typeName = null;
         bulkSize = 100;
         return;
      }
      
      // Deal with index settings if provided.
      if (settings.settings().containsKey("index")) {
         Map<String, Object> indexSettings = (Map<String, Object>)settings.settings().get("index");
         
         indexName = XContentMapValues.nodeStringValue(
               indexSettings.get("index"), riverName.name());
         typeName = XContentMapValues.nodeStringValue(
               indexSettings.get("type"), S3RiverUtil.INDEX_TYPE_DOC);
         bulkSize = XContentMapValues.nodeLongValue(
               indexSettings.get("bulk_size"), 100);
      } else {
         indexName = riverName.name();
         typeName = S3RiverUtil.INDEX_TYPE_DOC;
         bulkSize = 100;
      }
   }
   
   @Override
   public void start(){
      if (logger.isInfoEnabled()){
         logger.info("Starting amazon s3 river scanning");
      }
   }
   
   @Override
   public void close(){
      if (logger.isInfoEnabled()){
         logger.info("Closing amazon s3 river");
      }
      closed = true;
   }
   
   /** */
   private class S3Scanner implements Runnable{
      
      private BulkRequestBuilder bulk;
      
      @Override
      public void run(){
         while (true){
            if (closed){
               return;
            }
            
            try{
               bulk = client.prepareBulk();
               
               // If some bulkActions remains, we should commit them
               commitBulk();
            } catch (Exception e){
               logger.warn("Error while indexing content from {}", "");
               if (logger.isDebugEnabled()){
                  logger.debug("Exception for folder {} is {}", "", e);
                  e.printStackTrace();
               }
            }
         }
      }
      
      /**
       * Commit to ES if something is in queue
       * @throws Exception
       */
      private void commitBulk() throws Exception{
         if (bulk != null && bulk.numberOfActions() > 0){
            if (logger.isDebugEnabled()){
               logger.debug("ES Bulk Commit is needed");
            }
            BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()){
               logger.warn("Failed to execute " + response.buildFailureMessage());
            }
         }
      }
      
      /**
       * Commit to ES if we have too much in bulk 
       * @throws Exception
       */
      private void commitBulkIfNeeded() throws Exception {
         if (bulk != null && bulk.numberOfActions() > 0 && bulk.numberOfActions() >= bulkSize){
            if (logger.isDebugEnabled()){
               logger.debug("ES Bulk Commit is needed");
            }
            
            BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()){
               logger.warn("Failed to execute " + response.buildFailureMessage());
            }
            
            // Reinit a new bulk.
            bulk = client.prepareBulk();
         }
      }
      
      /** Add to bulk an IndexRequest. */
      private void esIndex(String index, String type, String id, XContentBuilder xb) throws Exception{
         if (logger.isDebugEnabled()){
            logger.debug("Indexing in ES " + index + ", " + type + ", " + id);
         }
         if (logger.isTraceEnabled()){
            logger.trace("Json indexed : {}", xb.string());
         }
         
         bulk.add(client.prepareIndex(index, type, id).setSource(xb));
         commitBulkIfNeeded();
      }

      /** Add to bulk a DeleteRequest. */
      private void esDelete(String index, String type, String id) throws Exception{
         if (logger.isDebugEnabled()){
            logger.debug("Deleting from ES " + index + ", " + type + ", " + id);
         }
         bulk.add(client.prepareDelete(index, type, id));
         commitBulkIfNeeded();
      }
   }
}
