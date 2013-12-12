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
package com.github.lbroudoux.elasticsearch.river.s3.rest;

import java.io.IOException;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
/**
 * REST actions definition for starting and stopping an Amazon S3 river.
 * @author laurent
 */
public class S3ManageAction extends BaseRestHandler{

   /** The constant for 'start river' command. */
   public static final String START_COMMAND = "_start";
   /** The constant for 'stop river' command. */
   public static final String STOP_COMMAND = "_stop";
   
   @Inject
   public S3ManageAction(Settings settings, Client client, RestController controller){
      super(settings, client);

      // Define S3 REST endpoints.
      controller.registerHandler(Method.GET, "/_s3/{rivername}/{command}", this);
   }
   
   @Override
   public void handleRequest(RestRequest request, RestChannel channel){
      if (logger.isDebugEnabled()){
         logger.debug("REST S3ManageAction called");
      }
      
      String rivername = request.param("rivername");
      String command = request.param("command");
      
      String status = null;
      if (START_COMMAND.equals(command)){
         status = "STARTED";
      } else if (STOP_COMMAND.equals(command)){
         status = "STOPPED";
      }
      
      try{
         if (status != null){
            XContentBuilder xb = jsonBuilder()
               .startObject()
                  .startObject("amazon-s3")
                     .field("feedname", rivername)
                     .field("status", status)
                  .endObject()
               .endObject();
            client.prepareIndex("_river", rivername, "_s3status").setSource(xb).execute().actionGet();
         }
         
         XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
         builder
            .startObject()
               .field(new XContentBuilderString("ok"), true)
            .endObject();
         channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
      } catch (IOException e) {
         onFailure(request, channel, e);
      }
   }
   
   /** */
   protected void onFailure(RestRequest request, RestChannel channel, Exception e){
      try{
          channel.sendResponse(new XContentThrowableRestResponse(request, e));
      } catch (IOException ioe){
         logger.error("Sending failure response fails !", e);
      }
   }
}
