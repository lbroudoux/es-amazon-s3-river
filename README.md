es-amazon-s3-river
==================

Amazon S3 river for Elasticsearch

This river plugin helps to index documents from a Amazon S3 account buckets.

*WARNING*: For 0.0.1 released version, you need to have the [Attachment Plugin](https://github.com/elasticsearch/elasticsearch-mapper-attachments).

*WARNING*: Starting from 0.0.2, you don't need anymore the [Attachment Plugin](https://github.com/elasticsearch/elasticsearch-mapper-attachments) as we use now directly [Tika](http://tika.apache.org/), see [issue #2](https://github.com/lbroudoux/es-amazon-s3-river/issues/2).

Versions
--------

<table>
   <thead>
      <tr>
         <td>Amazon S3 River Plugin</td>
         <td>ElasticSearch</td>
         <td>Attachment Plugin</td>
         <td>Tika</td>
      </tr>
   </thead>
   <tbody>
      <tr>
         <td>master (0.0.4-SNAPSHOT)</td>
         <td>1.0.0</td>
         <td>No more used</td>
         <td>1.4</td>
      </tr>
      <tr>
         <td>0.0.3</td>
         <td>1.0.0</td>
         <td>No more used</td>
         <td>1.4</td>
      </tr>
      <tr>
         <td>0.0.2</td>
         <td>0.90.0</td>
         <td>No more used</td>
         <td>1.4</td>
      </tr>
      <tr>
         <td>0.0.1</td>
         <td>0.90.0</td>
         <td>1.7.0</td>
         <td></td>
      </tr>
   </tbody>
</table>

Build Status
------------

Travis CI [![Build Status](https://travis-ci.org/lbroudoux/es-amazon-s3-river.png?branch=master)](https://travis-ci.org/lbroudoux/es-amazon-s3-river)


Getting Started
===============

Installation
------------

Just install as a regular Elasticsearch plugin by typing :

```sh
$ bin/plugin -install com.github.lbroudoux.elasticsearch/amazon-s3-river/0.0.3
```

This will do the job...

```
-> Installing com.github.lbroudoux.elasticsearch/amazon-s3-river/0.0.3...
Trying http://download.elasticsearch.org/com.github.lbroudoux.elasticsearch/amazon-s3-river/amazon-s3-river-0.0.3.zip...
Trying http://search.maven.org/remotecontent?filepath=com/github/lbroudoux/elasticsearch/amazon-s3-river/0.0.3/amazon-s3-river-0.0.3.zip...
Downloading ......DONE
Installed amazon-s3-river
```


Get Amazon AWS credentials (accessKey and secretKey)
------------------------------------------

First, you need to login to Amazon AWS account owning the S3 bucket to and then retrieve your security credentials by visiting this [page](https://portal.aws.amazon.com/gp/aws/securityCredentials).

Once done, you should note your `accessKey` and `secretKey` codes.


Creating an Amazon S3 river
------------------------

We create first an index to store our *documents* (optional):

```sh
$ curl -XPUT 'http://localhost:9200/mys3docs/' -d '{}'
```

We create the river with the following properties :

* accessKey : AAAAAAAAAAAAAAAA
* secretKey: BBBBBBBBBBBBBBBB
* Amazon S3 bucket to index : `myownbucket` 
* Path prefix to index in this buckets : `Work/` (This is optional. If specified, it should be an existing path with the trailing /)
* Update Rate : every 15 minutes (15 * 60 * 1000 = 900000 ms)
* Get only docs like `*.doc` and `*.pdf`
* Don't index `*.zip` and `*.gz`

```sh
$ curl -XPUT 'http://localhost:9200/_river/mys3docs/_meta' -d '{
  "type": "amazon-s3",
  "amazon-s3": {
    "accessKey": "AAAAAAAAAAAAAAAA",
    "secretKey": "BBBBBBBBBBBBBBBB",
    "name": "My Amazon S3 feed",
    "bucket" : "myownbucket"
    "pathPrefix": "Work/",
    "update_rate": 900000,
    "includes": "*.doc,*.pdf",
    "excludes": "*.zip,*.gz"
  }
}'
```

By default, river is using an index that have the same name (`mys3docs` in the above example).

*From 0.0.2 version*

The `source_url` of documents is now stored within Elasticsearch index in order to allow you to access
later the whole document content from your application (this is indeed a use case coming from [Scrutmydocs](http://www.scrutmydocs.org)).

By default, the plugin uses what is called the *resourceUrl* of a S3 bucket document. If the document have
been made public within S3, it can be accessed directly from your browser. If it's not the case, the stored url
is intended to be used by a regular S3 client that has the allowed set of credentials to access the document.

Another option to easily distribute S3 content is to setup a Web proxy in front of S3 such as CloudFront (see 
[Service Private Content With CloudFront](http://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/PrivateContent.html)).
In that later case, you'll want to rewrite `source_url` by substituting the S3 part by your own host name. This
plugin allows you to do that by specifying a `download_host` as a river properties.


Specifying index options
------------------------

Index options can be specified when creating an amazon-s3-river. The properties are the following :

* Index name : "amazondocs"
* Type of documents : "doc"
* Size of an indexation bulk : 50 (default is 100)

You'll have to use them as follow when creating a river :

```sh
$ curl -XPUT 'http://localhost:9200/_river/mys3docs/_meta' -d '{
  "type": "amazon-s3",
  "amazon-s3": {
    "accessKey": "AAAAAAAAAAAAAAAA",
    "secretKey": "BBBBBBBBBBBBBBBB",
    "name": "My Amazon S3 feed",
    "bucket" : "myownbucket"
    "pathPrefix": "Work/",
    "update_rate": 900000,
    "includes": "*.doc,*.pdf",
    "excludes": "*.zip,*.gz"
  },
  "index": {
    "index": "amazondocs",
    "type": "doc",
    "bulk_size": 50
  }
}'
```

Indexing Json documents
-----------------------

*From 0.0.4 version*

If you want to index Json files directly without parsing them through Tika, you can set the `json_support` configuration
option to `true` like 

```sh
$ curl -XPUT 'http://localhost:9200/_river/mys3docs/_meta' -d '{
  "type": "amazon-s3",
  "amazon-s3": {
    "accessKey": "AAAAAAAAAAAAAAAA",
    "secretKey": "BBBBBBBBBBBBBBBB",
    "name": "My Amazon S3 feed",
    "bucket" : "myownbucket"
    "pathPrefix": "Jsons/",
    "update_rate": 900000,
    "json_support": true,
    "includes": "*.json"
  }
}'
```

Be sure in your river configuration to correclty use `includes` or `excludes` to only retrieve Json documents.

In this case of `json_support` and if you did not define a mapping prior creating it, this river *will not*
automatically generate a mapping like mentioned below into the Advanced section. In this case, Elasticsearch will auto
guess the mapping.


Advanced
========

Management actions
------------------

If you need to stop a river, you can call the `_s3` endpoint with your river name followed by the `_stop` command like this :

```sh
GET _s3/mys3docs/_stop
```

To restart the river from the previous point, just call the corresponding `_start` endpoint :

```sh
GET _s3/mys3docs/_start
```

Autogenerated mapping
---------------------

When the river detect a new type, it creates automatically a mapping for this type.

```javascript
{
  "doc" : {
    "properties" : {
      "title" : {
        "type" : "string",
        "analyzer" : "keyword"
      },
      "modifiedDate" : {
        "type" : "date",
        "format" : "dateOptionalTime"
      },
      "file" : {
        "type" : "attachment",
        "fields" : {
          "file" : {
            "type" : "string",
            "store" : "yes",
            "term_vector" : "with_positions_offsets"
          },
          "title" : {
            "type" : "string",
            "store" : "yes"
          }
        }
      }
    }
  }
}
``` 

*From 0.0.2 version*

We now use directly Tika instead of the mapper-attachment plugin.

```javascript
{
  "doc" : {
    "properties" : {
      "title" : {
        "type" : "string",
        "analyzer" : "keyword"
      },
      "modifiedDate" : {
        "type" : "date",
        "format" : "dateOptionalTime"
      },
      "source_url" : {
        "type" : "string"
      },
      "file" : {
        "properties" : {
          "file" : {
            "type" : "string",
            "store" : "yes",
            "term_vector" : "with_positions_offsets"
          },
          "title" : {
            "type" : "string",
            "store" : "yes"
          }
        }
      }
    }
  }
}
``` 
     
    
License
=======

```
This software is licensed under the Apache 2 license, quoted below.

Copyright 2013 Laurent Broudoux

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
```
