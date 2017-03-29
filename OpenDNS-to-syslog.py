from __future__ import print_function

import json
import urllib
import boto3
import socket
import csv
from StringIO import StringIO
from dateutil import parser

from zlib import decompress
from zlib import MAX_WBITS

s3 = boto3.client('s3')

HOSTNAME="opendns"

SYSLOG_HOST='10.0.0.113'
SYSLOG_PORT=514
SYSLOG_TCP = False


def lambda_handler(event, context):
    if SYSLOG_TCP:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect( (SYSLOG_HOST, SYSLOG_PORT) )
    else:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    message_count = 0

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    try:
        print(key)
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read()
        csv_file =  decompress( body, 32+MAX_WBITS)

        for line in csv.reader( StringIO(csv_file) ):
            timestamp = parser.parse(line[0]).strftime('%b %e %H:%M:%S')
            message = ','.join( '"{}"'.format(m) for m in line )
            syslog_msg = "<13>{} {} {}".format(timestamp, HOSTNAME, message )

            if SYSLOG_TCP:
                sock.send( syslog_msg )
            else:
                sock.sendto( syslog_msg, (SYSLOG_HOST, SYSLOG_PORT) )
            message_count += 1

        print("Sent {} messages to {}:{}".format( message_count, SYSLOG_HOST, SYSLOG_PORT))

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    if SYSLOG_TCP:
        sock.close()

