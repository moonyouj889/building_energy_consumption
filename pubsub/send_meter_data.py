#!/usr/bin/env python

# TODO: fix license statement
# Copyright 2019 Julie Jung <moonyouj889@gmail.com>
#
# The following code is a derivative work of the code
# from the GCP training-data-analyst repo,
# which is licensed under the Apache License, Version 2.0

import datetime
import argparse
import logging
from google.cloud import pubsub
import gzip
import time
# for python2
# import dateutil.parser

TOPIC = 'energy'
INPUT = './chrono-comma-sep-buildings-energy-consumption-clean-data.csv.gz'


def iso_to_datetime(timestamp):
    # for python2
    # return dateutil.parser.parse(timestamp)
    return datetime.datetime.fromisoformat(timestamp)


def get_timestamp(row):
    # convert from bytes to str and look at first field of row
    timestamp = row.decode('utf-8').split(',')[0]
    # convert from isoformat to regular datetime
    return iso_to_datetime(timestamp)


def peek_timestamp(ifp):
    # peek ahead to next line, get timestamp and go back
    pos = ifp.tell()
    line = ifp.readline()
    ifp.seek(pos)
    return get_timestamp(line)


def peek_timestamp(data):
    # peek ahead to next line, retrieve timestamp and go back to top
    # since GzipFile.readline() moves onto the next line position
    pos = data.tell()
    row = data.readline()
    data.seek(pos)
    return get_timestamp(row)


def publish(publisher, topic, messages):
    # publish to a topic in pubsub
    num_of_events = len(messages)
    if num_of_events > 0:
        logging.info('Publishing {} row(s) starting on {}'.format(
            num_of_events, get_timestamp(messages[0])))
        for row in messages:
            publisher.publish(topic, row)


def splitRow(row, columnNames):
    messagesToAdd = []
    columnNamesList = columnNames.split(',')
    data = row.split(',')
    timestamp, building_id, prev_i = (data[0], 1, 1)
    for i in range(1, len(columnNamesList)):
        # next_id = int(columnNamesList[i][4])
        # if building_id != next_id:
        if columnNamesList[i] == 'Gen' and i > 1:
            messagesToAdd.append(','.join([timestamp, str(building_id)] + data[prev_i:i]))
            prev_i = i
            building_id += 1
    messagesToAdd.append(','.join([timestamp, str(building_id)] + data[prev_i:]))
    return messagesToAdd


def simulate(topic, meterData, firstObsTime, programStart, speedFactor, columnNames):
    # compute how many seconds you have to wait
    # to match the data's time increments to actual time passing
    def get_seconds_to_match(event_time):
        # how many seconds passed since start of simulation
        real_delta_t = (datetime.datetime.utcnow() - programStart).seconds
        # if speedFactor = 1, you simulate at the same rate as data's delta t
        sim_delta_t = (event_time - firstObsTime).seconds / speedFactor
        seconds_to_match = sim_delta_t - real_delta_t
        return seconds_to_match

    messages = []

    for row in meterData:
        # retrieve timestamp of current row
        event_time = get_timestamp(row)

        # if there should be waiting,
        if get_seconds_to_match(event_time) > 0:
            # publish the accumulated messages
            publish(publisher, topic, messages)
            messages = []  # empty out messages to send

            # recompute wait time, since publishing takes time
            seconds_to_match = get_seconds_to_match(event_time)
            # despite waiting for publishing, if there still are seconds to match,
            if seconds_to_match > 0:
                logging.info('Sleeping {} seconds'.format(seconds_to_match))
                # wait for real time to match the event time
                time.sleep(seconds_to_match)
        # if waiting time less than a second, can add more messages to send
        # split row splits the original data by building id
        # to simulate a scenario where the IoT Gateway only 
        # accumulates data by building prior to publishing on PubSub
        messagesToAdd = splitRow(row, columnNames) 
        messages = messages + messagesToAdd

    # left-over records; notify again
    publish(publisher, topic, messages)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Send general meter and sub meter data to Cloud Pub/Sub in small groups,' +
        'simulating streaming data')
    # this argument exists so testing won't take 15 minutes at least for one row
    parser.add_argument(
        '--speedFactor', help='Ex) 60 => 1 hr of data in 1 min', required=True, type=float)
    parser.add_argument(
        '--project', help='Ex) --project $PROJECT_ID', required=True)
    args = parser.parse_args()

    # logging status of data simulation
    logging.basicConfig(
        format='%(levelname)s: %(message)s', level=logging.INFO)

    # establish pub/sub client
    publisher = pubsub.PublisherClient()
    topic_path = publisher.topic_path(args.project, TOPIC)

    # check if topic was created, else create the topic
    try:
        publisher.get_topic(topic_path)
        logging.info('Found pub/sub topic {}'.format(TOPIC))
    except:
        publisher.create_topic(topic_path)
        logging.info('Creating pub/sub topic {}'.format(TOPIC))

    # notify about each line in the input file
    programStartTime = datetime.datetime.utcnow()
    with gzip.open(INPUT, 'rb') as meter_data:
        columnNames = meter_data.readline()  # skip row of column names
        firstObsTime = peek_timestamp(meter_data)
        logging.info('Sending sensor data from {}'.format(firstObsTime))
        simulate(topic_path, meter_data, firstObsTime,
                 programStartTime, args.speedFactor,  columnNames)
