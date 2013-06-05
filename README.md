es-amazon-s3-river
==================

Amazon S3 river for Elasticsearch

This river plugin helps to index documents from a Amazon S3 account buckets.

*WARNING*: You need to have the [Attachment Plugin](https://github.com/elasticsearch/elasticsearch-mapper-attachments).

Versions
--------

<table>
   <thead>
      <tr>
         <td>Amazon S3 River Plugin</td>
         <td>ElasticSearch</td>
         <td>Attachment Plugin</td>
      </tr>
   </thead>
   <tbody>
      <tr>
         <td>master (0.0.1-SNAPSHOT)</td>
         <td>0.90.0</td>
         <td>1.7.0</td>
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


Specifying index options
------------------------

Index options can be specified when creating an amazon-s3-river. The properties are the following :