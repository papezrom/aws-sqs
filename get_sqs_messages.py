#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Author: Roman Papezhuk

"""
Usage: get_sqs_messages.py --src=<QUEUE_URL> --dst=<QUEUE_URL> --file_path=<FILE_PATH> --each_exception=<true|false>
       get_sqs_messages.py -h | --help
"""

import json
import simplejson as json
import boto3
import docopt
import sys


sqs_client = boto3.client('sqs')
output = set()

def get_messages_from_queue(queue_url):

    messages = []

    while True:

        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=10
        )

        try:
            yield from resp['Messages']
        except KeyError:
           return

        entries = [
            {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
            for msg in resp['Messages']
        ]

        resp = sqs_client.delete_message_batch(
            QueueUrl=queue_url, Entries=entries
        )

        if len(resp['Successful']) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )


    return json.dumps(messages)


def write_to_file(file_path, data):
    try:
        report = open(file_path, "a")

        report.write("%s\n" % data)
        report.close()
    except KeyError:
        print(sys.exc_info())
        sys.exit(1)


if __name__ == '__main__':

    args = docopt.docopt(__doc__)
    src_queue_url = args['--src']
    dst_queue_url = args['--dst']
    file_path = args['--file_path']
    each_exception = args['--each_exception']


    for message in get_messages_from_queue(src_queue_url):

        data=message['Body']
        output.add(data)

        if each_exception == 'true':
            print('This is exception body', data)

            write_to_file(file_path, '\n === Exeption ===')
            write_to_file(file_path, data)

        sqs_client.send_message(
            QueueUrl=dst_queue_url,
            MessageBody=data
        )

    write_to_file(file_path, '\n ======= Summary report =======')
    for each in output:
        write_to_file(file_path, '\n')
        write_to_file(file_path, each)

exit(0)