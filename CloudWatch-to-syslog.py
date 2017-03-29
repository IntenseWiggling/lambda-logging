from __future__ import print_function
from base64 import b64decode
from zlib import decompress
from zlib import MAX_WBITS
import json
import socket

SYSLOG_HOST='10.0.0.113'
SYSLOG_PORT=514
SYSLOG_TCP = False

def lambda_handler(event, context):
    if SYSLOG_TCP:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect( (SYSLOG_HOST, SYSLOG_PORT) )
    else:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    data = event['awslogs']['data']
    jsonData = decompress( b64decode( data ), 16+MAX_WBITS)

    message_count = 0

    for log in json.loads(jsonData)['logEvents']:
        if SYSLOG_TCP:
            sock.send( log['message'] )
        else:
            sock.sendto( log['message'], (SYSLOG_HOST, SYSLOG_PORT) )
        message_count += 1

    if SYSLOG_TCP:
        sock.close()

    print("Sent {} messages to {}:{}".format( message_count, SYSLOG_HOST, SYSLOG_PORT))

    return True
