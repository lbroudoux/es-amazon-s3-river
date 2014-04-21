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

import static junit.framework.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
/**
 * Test case for S3RiverUtil class.
 * @author laurent
 */
public class S3RiverUtilTest{

    @Test
    public void shouldSayIsIndexable(){
        List<String> includes = Arrays.asList("*.pdf");
        List<String> excludes = Arrays.asList("*.mkv");
        assertTrue(S3RiverUtil.isIndexable("mydoc.pdf", includes, excludes));
    }

    @Test
    public void shouldNotSayIsIndexable(){
        List<String> includes = Arrays.asList("*.pdf");
        List<String> excludes = Arrays.asList("*.mkv");
        assertFalse(S3RiverUtil.isIndexable("mymovie.mkv", includes, excludes));
    }

    @Test
    public void shouldSayIsIndexableWhenNoSpec(){
        // mydoc not in inclusions.
        assertTrue(S3RiverUtil.isIndexable("mydoc.pdf", null, null));
    }

    @Test
    public void shouldSayIsIndexableWhenInclusionsOnly(){
        List<String> includes = Arrays.asList("*.pdf");
        List<String> excludes = Arrays.asList();
        // mydoc in inclusions.
        assertTrue(S3RiverUtil.isIndexable("mydoc.pdf", includes, excludes));
    }

    @Test
    public void shouldNotSayIsIndexableWhenInclusionsOnly(){
        List<String> includes = Arrays.asList("*.pdf");
        List<String> excludes = Arrays.asList();
        // mymovie not in inclusions.
        assertFalse(S3RiverUtil.isIndexable("mymovie.mkv", includes, excludes));
    }

    @Test
    public void shouldSayIsIndexableWhenExclusionsOnly(){
        List<String> includes = Arrays.asList();
        List<String> excludes = Arrays.asList("*.mkv");
        // mydoc not in exclusions.
        assertTrue(S3RiverUtil.isIndexable("mydoc.pdf", includes, excludes));
    }

    @Test
    public void shoudNotSayIsIndexableWhenExclusionsOnly(){
        List<String> includes = Arrays.asList();
        List<String> excludes = Arrays.asList("*.mkv");
        // mymovie in exclusions.
        assertFalse(S3RiverUtil.isIndexable("mymovie.mkv", includes, excludes));
    }
}
