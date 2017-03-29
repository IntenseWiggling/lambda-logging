#!/usr/bin/env python2

from __future__ import print_function

import json
import logging
import os

from base64 import b64decode
from zlib import decompress
from zlib import MAX_WBITS

from logging.handlers import SysLogHandler
from datetime import datetime

LOG_HOSTNAME = os.environ["LOGHOSTNAME"]
SYSLOG_HOST = os.environ["SYSLOGHOST"]
SYSLOG_PORT = int(os.environ["SYSLOGPORT"])

def lambda_handler(event, context):
    # Setup syslog
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    syslog_handle = SysLogHandler( address=(SYSLOG_HOST, SYSLOG_PORT) ) 
    formatter = logging.Formatter('%(syslogtime)s %(hostname)s %(message)s') 
    syslog_handle.setFormatter(formatter)
    logger.addHandler(syslog_handle)

    log_extra = dict()
    log_extra['hostname'] = LOG_HOSTNAME

    # Decompress and extract logs logs
    data = event['awslogs']['data']
    jsonData = decompress( b64decode( data ), 16+MAX_WBITS)
    
    # Loop through individual log events, send via syslog
    message_count = 0
    for event in json.loads(jsonData)['logEvents']:

        timestamp = int( event['timestamp'] ) / 1000.0
        message = event['message']

        log_extra['syslogtime'] = datetime.fromtimestamp( timestamp ).strftime('%b %e %H:%M:%S')

        logger.info( message, extra=log_extra
        
        message_count += 1
        
    print("Sent {} messages to {}:{} as {}.".format( message_count, SYSLOG_HOST, SYSLOG_PORT, LOG_HOSTNAME) )

