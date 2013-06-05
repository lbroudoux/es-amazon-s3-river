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
import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
/**
 * 
 * @author laurent
 */
public class S3RiverUtil{

   public static final String INDEX_TYPE_DOC = "doc";
   
   public static final String DOC_FIELD_TITLE = "title";
   public static final String DOC_FIELD_MODIFIED_DATE = "modifiedDate";
   
   /**
    * Extract array from settings (array or ; delimited String)
    * @param settings Settings
    * @param path Path to settings definition
    * @return Array of settings
    */
   @SuppressWarnings("unchecked")
   public static String[] buildArrayFromSettings(Map<String, Object> settings, String path){
      String[] includes;

      // We manage comma separated format and arrays
      if (XContentMapValues.isArray(XContentMapValues.extractValue(path, settings))) {
         List<String> includesarray = (List<String>) XContentMapValues.extractValue(path, settings);
         int i = 0;
         includes = new String[includesarray.size()];
         for (String include : includesarray) {
            includes[i++] = Strings.trimAllWhitespace(include);
         }
      } else {
         String includedef = (String) XContentMapValues.extractValue(path, settings);
         includes = Strings.commaDelimitedListToStringArray(Strings.trimAllWhitespace(includedef));
      }
      
      String[] uniquelist = Strings.removeDuplicateStrings(includes);
      
      return uniquelist;
   }

   /**
    * Tells if an Aamzon S3 file is indexable from its key (file name), based on includes
    * and excludes rules. 
    * @return true if file should be indexed, false otherwise
    */
   public static boolean isIndexable(String key, List<String> includes, List<String> excludes){
      // If no rules specified, we index everything !
      if ((includes == null && excludes == null) 
            || (includes.isEmpty() && excludes.isEmpty())){
         return true;
      }
      
      // Exclude rules : we know that whatever includes rules are, we should exclude matching files.
      if (excludes != null){
         for (String exclude : excludes){
            String regex = exclude.replace("?", ".?").replace("*", ".*?");
            if (key.matches(regex)){
               return false;
            }
         }
      }
      
      // Include rules : we should add document if it match include rules.
      if (includes == null || includes.isEmpty()){
         return true;
      }
      if (includes != null){
         for (String include : includes){
            String regex = include.replace("?", ".?").replace("*", ".*?");
            if (key.matches(regex)){
               return true;
            }
         }
      }
      
      return false;
   }
}
