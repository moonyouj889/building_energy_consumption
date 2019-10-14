#    Copyright 2019 Julie Jung <moonyouj889@gmail.com>

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

import datetime
import argparse
import logging
from google.cloud import pubsub
import gzip
import time
import sys

# due to python2 not have datetime.datetime.fromisoformat(),
# check before running of which version to load
if sys.version_info >= (3, 0):
    from datetimepy3 import iso_to_datetime
else:
    from datetimepy2 import iso_to_datetime


TOPIC = 'energy'
INPUT = './data/processed_data/chrono-comma-sep-buildings-energy-consumption-clean-data.csv.gz'
# energy was measured every 15 minutes in the original data
REAL_DATA_T_INCREMENT = 15 * 60


def get_timestamp(row):
    # convert from bytes to str and look at first field of row
    timestamp = row.decode('utf-8').split(',')[0]
    # convert from isoformat to regular datetime
    return iso_to_datetime(timestamp)


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


def splitRow(row, columnNames, timestamp, speedFactor):
    messagesToAdd = []
    columnNamesList = columnNames.split(',')
    data = row.split(',')
    building_id, prev_i = (1, 1)
    for i in range(1, len(columnNamesList)):
        if columnNamesList[i] == 'Gen' and i > 1:
            values = data[prev_i:i]
            # map(lambda value: value * (speedFactor/REAL_DATA_T_INCREMENT), values)
            messagesToAdd.append(
                ','.join([timestamp, str(building_id)] + values))
            prev_i = i
            # building_id = next_id
            building_id += 1
    messagesToAdd.append(
        ','.join([timestamp, str(building_id)] + data[prev_i:]))
    return messagesToAdd


def simulate(topic, meterData, firstObsTime, programStart, speedFactor, columnNames):
    # compute how many seconds you have to wait
    # to match the data's time increments to actual time passing
    # def get_seconds_to_match(event_time):
    #     # how many seconds passed since start of simulation
    #     real_delta_t = (datetime.datetime.utcnow() - programStart).seconds
    #     # if speedFactor = 1, you simulate at the same rate as data's delta t
    #     sim_delta_t = (event_time - firstObsTime).seconds / speedFactor
    #     seconds_to_match = sim_delta_t - real_delta_t
    #     return seconds_to_match

    # messages = []

    for row in meterData:
        # retrieve timestamp of current row
        event_time = datetime.datetime.utcnow()

        # if there should be waiting,
        # if get_seconds_to_match(event_time) > 0:
        #     # publish the accumulated messages
        #     publish(publisher, topic, messages)
        #     messages = []  # empty out messages to send

        #     # recompute wait time, since publishing takes time
        #     seconds_to_match = get_seconds_to_match(event_time)
        #     # despite waiting for publishing, if there still are seconds to match,
        #     if seconds_to_match > 0:
        #         logging.info('Sleeping {} seconds'.format(seconds_to_match))
        #         # wait for real time to match the event time
        #         time.sleep(seconds_to_match)
        # if waiting time less than a second, can add more messages to send
        # split row splits the original data by building id
        # to simulate a scenario where the IoT Gateway only
        # accumulates data by building prior to publishing on PubSub
        messagesToAdd = splitRow(
            row, columnNames, str(event_time), speedFactor)
        publish(publisher, topic, messagesToAdd)
        logging.info('Sleeping {} seconds'.format(speedFactor))
        # wait for real time to match the event time
        time.sleep(speedFactor)

        # messages = messages + messagesToAdd

    # left-over records; notify again
    publish(publisher, topic, messagesToAdd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Send general meter and sub meter data to Cloud Pub/Sub in small groups,' +
        'simulating streaming data')
    # this argument exists so testing won't take 15 minutes at least for one row
    parser.add_argument(
        '--speedFactor', required=True, type=int,
        help='in second basis: Ex) 60 => one event per minute, 900 => one event per 15 minutes')
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
