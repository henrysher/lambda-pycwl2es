#!/usr/bin/env python
"""
/* This file is under the Modified MIT License.  
 *
 * Copyright 2018 Henry Huang, henry.s.huang@gmail.com, All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
------------------------------------------------------------------------------

This Lambda function is set up as a subscription from a Cloudwatch Logs group to 
ElasticSearch service. As new logs are written to the log group, they're sent to
this Lambda, which unzips them, pulls out the log messages, and writes them to 
the desired domain in ElasticSearch service.

The index name convention is: cwl-YYYY.MM.DD.

Set up the following as Lambda environment variables:
    region        region name for AWS, for example: cn-north-1
    baseUrl       base URL for current AWS region, for example: amazonaws.com.cn
    esDomain      domain name for ElasticSearch service
    
They can be set via console, CLI or CF template. E.g.:
        aws lambda create-function \
          --region us-east-1
          --function-name myFunction
          --zip-file file://path/package.zip
          --role role-arn
          --environment Variables={bucket1=mybucket1}
          --handler index.handler
          --runtime python2.7
          --profile default   

@author Henry Huang
@date December 2018
"""

import boto3
import logging
import json
import gzip
import time
import os
import sys
import base64
import hashlib
import hmac
import httplib
from datetime import datetime
from StringIO import StringIO
from botocore.exceptions import ClientError

# Sample variables
region = 'cn-north-1'
baseUrl = 'amazonaws.com.cn'
esDomain = 'search-xxx-yayzk5djz4xgur4tsd7yjyytt'

# Sample event
event = { "awslogs": { "data": "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" } }

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

def getSignatureKey(key, date_stamp, regionName, serviceName):
    kDate = sign(('AWS4' + key).encode('utf-8'), date_stamp)
    kRegion = sign(kDate, regionName)
    kService = sign(kRegion, serviceName)
    kSigning = sign(kService, 'aws4_request')
    return kSigning

def writeit(region, baseUrl, esDomain, payLoad):

    service = 'es'
    esEndpoint = '.'.join([esDomain, region, service, baseUrl])
    credentials = boto3.Session().get_credentials()

    method = 'POST'
    content_type = 'application/json'
    amz_date = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    date_stamp = datetime.utcnow().strftime('%Y%m%d')
    canonical_uri = '/_bulk'
    canonical_querystring = ''
    canonical_headers = 'content-type:' + content_type + '\n' + 'host:' + esEndpoint + '\n' + 'x-amz-date:' + amz_date + '\n'
    signed_headers = 'content-type;host;x-amz-date'
    payload_hash = hashlib.sha256(payLoad.encode('utf-8')).hexdigest()
    canonical_request = method + '\n' + canonical_uri + '\n' + canonical_querystring + '\n' + canonical_headers + '\n' + signed_headers + '\n' + payload_hash
    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = date_stamp + '/' + region + '/' + service + '/' + 'aws4_request'
    string_to_sign = algorithm + '\n' +  amz_date + '\n' +  credential_scope + '\n' +  hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
    signing_key = getSignatureKey(credentials.secret_key, date_stamp, region, service)
    signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()
    authorization_header = algorithm + ' ' + 'Credential=' + credentials.access_key + '/' + credential_scope + ', ' +  'SignedHeaders=' + signed_headers + ', ' + 'Signature=' + signature
    headers = {'Content-Type':content_type, 
                'Host': esEndpoint,
                'X-Amz-Date':amz_date,
                'X-Amz-Security-Token': credentials.token,
                'Authorization':authorization_header}
    print(payLoad)
    print(headers)
    conn = httplib.HTTPSConnection(esEndpoint)
    conn.request(method, canonical_uri, payLoad, headers)
    response = conn.getresponse()
    print response.status, response.reason
    data = response.read()
    print data

def lambda_handler(event, context):
    print(json.dumps(event))

    try:
        region = os.environ['region']
        baseUrl = os.environ['baseUrl']
        esDomain = os.environ['esDomain']
    except:
        print("FATAL ERROR. Lambda environment variable 'region', 'baseUrl' or 'esDomain' not set.")
        return "failed" 
    print("Using: region " + region)
    print("Using: baseUrl " + baseUrl)
    print("Using: esDomain " + esDomain)

    #capture the CloudWatch log data
    outEvent = str(event['awslogs']['data'])
    
    #decode and unzip the log data
    outEvent = gzip.GzipFile(fileobj=StringIO(outEvent.decode('base64','strict'))).read()
    
    #convert the log data from JSON into a dictionary
    cleanEvent = json.loads(outEvent)
    #print(str(cleanEvent))
    if (('messageType' in cleanEvent) and cleanEvent['messageType'] == 'CONTROL_MESSAGE'):
        print "Control Message, skip now..."
        return "skip"

    logGroup = cleanEvent['logGroup']
    logStream = cleanEvent['logStream']
    logEvents = cleanEvent['logEvents']
    logOwner = cleanEvent['owner']

    bulkRequestBody = ''

    for logEvent in logEvents:

        source = {}
        if 'extractedFields' in logEvent:
            fields = logEvent['extractedFields']
            for key in fields:
                value = fields[key]
                if value.isdigit():
                    source[key] = int(value)
                    continue
                elif value.startswith('{'):
                    try:
                        jsonValue = json.loads(value)
                        source['$' + key] = jsonValue
                        continue
                    except:
                        pass
                source[key] = value

        timestamp = logEvent['timestamp'] / 1000
        dateTime = datetime.fromtimestamp(timestamp)
        
        source['@id'] = logEvent['id']
        source['@timestamp'] = str(timestamp)
        source['@message'] = logEvent['message']
        source['@owner'] = logOwner
        source['@log_group'] = logGroup
        source['@log_stream'] = logStream

        # index name format: cwl-YYYY.MM.DD
        indexName = '.'.join([
            'cwl-' + str(dateTime.year),      # year
            ('0' + str(dateTime.month))[-2:],  # month
            ('0' + str(dateTime.day))[-2:]    # day
        ])
        action = { "index": {} }
        action['index']['_index'] = indexName
        action['index']['_type'] = logGroup
        action['index']['_id'] = logEvent['id']
        
        bulkRequestBody += '\n'.join([ 
            json.dumps(action), 
            json.dumps(source),
        ]) + '\n'
        
    # Write it out again
    writeit(region, baseUrl, esDomain, bulkRequestBody)

    #log ending
    print "Ending now..."
    return "success"

# Test harness    
if __name__ == "__main__":
    print("starting main")
    lambda_handler(event, "")
    print("ending main")

