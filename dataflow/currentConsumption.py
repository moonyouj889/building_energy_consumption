#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import json
import csv
import argparse
import os

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.pipeline import PipelineOptions
from apache_beam.transforms.combiners import Mean



# data gets collected 4 times per hour (every 15 minutes)
DATA_COLLECTION_FREQUENCY = 4
ROWS_PER_DAY = 10
# ROWS_PER_DAY = DATA_COLLECTION_FREQUENCY * 24
SCHEMA_PATH = 'data/processed_data/bq_schemas.txt'


class BQTranslateTransformation:
    '''A helper class which contains the logic to translate the file into a
  format BigQuery will accept.'''

    def __init__(self):
        # load_schema taken from json file extracted from processCSV.py 
        # in a realistic scenario, you won't be able to automate it like this.
        # and probably have to manually insert schema
        dir_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        schema_file = os.path.join(dir_path, SCHEMA_PATH) 
        with open(schema_file) as bq_schema_file:
            self.schemas = json.load(bq_schema_file)
        self.stream_schema = {'fields':[
                                {'name': 'timestamp', 
                                 'type': 'TIMESTAMP', 
                                 'mode':'REQUIRED'},
                                {'name': 'building_id',
                                 'type': 'INTEGER',
                                 'mode': 'REQUIRED'},
                                {'name': 'Gen_Avg',
                                 'type': 'INTEGER',
                                 'mode': 'REQUIRED'}]}


    def parse_method_load(self, string_input):
        '''This method translates a single line of comma separated values to a
        dictionary which can be loaded into BigQuery.
        Args:
            string_input: A comma separated list of values in the form of
            timestamp,building id,general meter reading, and variable size of sub meter readings
                ex1)2017-03-31T20:00:00-04:00,1,6443.0,1941.0,40.0
                ex2)2017-03-31T20:00:00-04:00,2,5397.0,2590.0
        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input. Deciding which schema to use by building_id.
            The schemas of 8 buildings can be retrieved from bq_schema.txt, 
            produced by processCSV.py and saved onto self.schemas

                ex1)
                    {'timestamp': '2017-03-31T20:00:00-04:00',
                    'building_id': 1,
                    '1_Gen': 6443,
                    '1_Sub_1': 1941,
                    '1_Sub_14': 40
                    }
                ex2)
                    {'timestamp': '2017-03-31T20:00:00-04:00',
                    'building_id': 2,
                    '2_Gen': 5397,
                    '2_Sub_1': 2590
                    }
        '''
        row = {}
        schema = None
        i = 0
        values = string_input.split(',')
        for value in values:
            # if at first column, add the timestamp, 
            #which is the same format no matter the building
            if i == 0: fieldName = 'timestamp'
            # first check what the building_id is, which is always the 2nd column
            elif i == 1:
                schema = self.schemas[int(value)-1]['fields']
                fieldName = 'building_id'
            # then retrieve the corresponding schema
            # and then match the values with field numbers to add to the dictionary
            else:
                fieldName = schema[i]['name']
            row[fieldName] = value
            i += 1
        logging.info('pasedRow: {}'.format(row))
        return row


    def parse_method_stream(self, k, v):
        ''' Same as parse_method_load(), but for hourly averages of each sensor, 
        combined to one table

        Args:
            k,v pair of building Id and main meter reading
        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from (k, v). The timestamp uses the current time 
            (when the aggregation is calculated) instead of matching to the fake time
            in case of using this logic for real time data.

                {'timestamp': [Actual Time Right Now],
                'building_id': 1,
                'Gen_Avg': 6443}
        '''
        return {'timestamp': datetime.datetime.utcnow(),
                'building_id': int(k),
                'Gen_Avg': int(v)}


def run(argv=None):
    '''Build and run the pipeline.'''
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic', dest='input_topic', required=True,
        help=('Input PubSub topic of the form '
              '"projects/<PROJECT>/topics/<TOPIC>".'))
    parser.add_argument(
        '--output_load_table_suffix', dest='output_l', required=True,
        help=('Output BQ table to write results to (suffix). "[datasetID].[tableID]".' + 
              'Since we have 8 buildings, each building ' +
              'will be loaded on the corresponding table. ex) given argument, "energy.building" ' +
              'building 1\'s data will be loaded in energy.building1 ' ))
    parser.add_argument(
        '--output_stream_table', dest='output_s', required=True,
        help='Output BQ table to write results to. "[datasetID].[tableID]"')
    parser.add_argument(
        '--output_topic', required=True,
        help=('Output PubSub topic of the form ' +
              '"projects/<PROJECT>/topics/<TOPIC>".' +
              'ex) "projects/building-energy-consumption/' +
              'topics/energy_stream"')
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    #logging.info('parsed args: {}'.format(known_args))
    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line.  This includes information like where Dataflow should
    # store temp files, and what the project id is.
    pipeline_options = PipelineOptions(pipeline_args)
    p = beam.Pipeline(options=pipeline_options)
    # schema = parse_table_schema_from_json(data_ingestion.schema_str)

    rowToBQ = BQTranslateTransformation()

    # with open(SCHEMA_PATH) as bq_schema_file:
    #     load_schema = json.load(load_schema_file)
    #     stream_schema = json.load(load_schema_file)
    ''' 
    if new columns need to be added, add by
    [SCHEMATYPE]_schema['fields'].append({
        'name': [FIELDNAME],
        'type': [FIELDTYPE],
        'mode': [FIELDMODE],
    })
    '''

    # ingest pubsub messages, extract data, and save to lines
    # so it can be used by both batch ingest and stream aggregations
    lines = (p 
             | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                    topic=known_args.input_topic).with_output_types(bytes) 
             | 'ConvertFromBytesToStr' >> beam.Map(lambda b: b.decode('utf-8'))
            )

    # split to streaming inserts and batch load 
    # because load is free and stream inserts costs money by size of data

    # Convert row of str to BQ rows, and load batch data to table on a daily basis
    # Set batch_size to rows per day to load sensor data in BQ on a daily basis
    # batch_size is a number of rows to be written to BQ per streaming API insert. 
    rows = (lines | 'StringToBigQueryRowLoad' >> beam.Map(lambda s: rowToBQ.parse_method_load(s)))

    # load_schema taken from json file extracted from processCSV.py
    # In a realistic scenario, you won't be able to automate it like this,
    # but probably have to manually insert schema
    load_schema = rowToBQ.schemas

    # filter and load into 8 tables based off of the given table suffix argument
    load1 = (rows | 'FilterBuilding1' >> beam.Filter(lambda row: int(row['building_id']) == 1)
                  | 'B1BQLoad' >> beam.io.WriteToBigQuery(
                                    table = known_args.output_l + '1',
                                    schema = load_schema[0], batch_size = ROWS_PER_DAY))
    load2 = (rows | 'FilterBuilding2' >> beam.Filter(lambda row: int(row['building_id']) == 2)
                  | 'B2BQLoad' >> beam.io.WriteToBigQuery(
                                    table = known_args.output_l + '2',
                                    schema = load_schema[1],batch_size = ROWS_PER_DAY))
    load3 = (rows | 'FilterBuilding3' >> beam.Filter(lambda row: int(row['building_id']) == 3)
                  | 'B3BQLoad' >> beam.io.WriteToBigQuery(
                                    table = known_args.output_l + '3',
                                    schema = load_schema[2],batch_size = ROWS_PER_DAY))
    load4 = (rows | 'FilterBuilding4' >> beam.Filter(lambda row: int(row['building_id']) == 4)
                  | 'B4BQLoad' >> beam.io.WriteToBigQuery(
                                    table = known_args.output_l + '4',
                                    schema = load_schema[3],batch_size = ROWS_PER_DAY))
    load5 = (rows | 'FilterBuilding5' >> beam.Filter(lambda row: int(row['building_id']) == 5)
                  | 'B5BQLoad' >> beam.io.WriteToBigQuery(
                                    table = known_args.output_l + '5',
                                    schema = load_schema[4],batch_size = ROWS_PER_DAY))
    load6 = (rows | 'FilterBuilding6' >> beam.Filter(lambda row: int(row['building_id']) == 6)
                  | 'B6BQLoad' >> beam.io.WriteToBigQuery(table = known_args.output_l + '6',
                                schema = load_schema[5],batch_size = ROWS_PER_DAY))
    load7 = (rows | 'FilterBuilding7' >> beam.Filter(lambda row: int(row['building_id']) == 7)
                  | 'B7BQLoad' >> beam.io.WriteToBigQuery(
                                    table = known_args.output_l + '7',
                                    schema = load_schema[6],batch_size = ROWS_PER_DAY))
    load8 = (rows | 'FilterBuilding8' >> beam.Filter(lambda row: int(row['building_id']) == 8)
                  | 'B8BQLoad' >> beam.io.WriteToBigQuery(
                                    table = known_args.output_l + '8',
                                    schema = load_schema[7],batch_size = ROWS_PER_DAY))
    
    # stream aggregation pipeline; saved to avgs
    # to be used for writing to BigQuery and publishing to Pubsub
    # sliding window of 1 hour, period of 15 minutes
    # TODO: beam.ParDo(GroupByKey()) to put meters together
    avgs = (lines
             | 'SetTimeWindow' >> beam.WindowInto(window.SlidingWindows(3600, 900, offset=0))
             # splitting to k,v of buildingId (2nd column), general meter reading (3rd column)
             | 'ByBuilding' >> beam.Map(lambda s: (s.split(',')[1], int(s.split(',')[2])))
             | 'GetAvgByBuilding' >> Mean.PerKey())
    
    '''
    classapache_beam.transforms.window.FixedWindows(size, offset=0)
    size
    Size of the window as seconds.
    offset
    Offset of this window as seconds since Unix epoch. 
    Windows start at t=N * size + offset where t=0 is the epoch. 
    The offset must be a value in range [0, size). 
    If it is not it will be normalized to this range.
    '''
    
    # Convert row of str to BigQuery rows, and append to the BQ table.
    (avgs | 'KVToBigQueryRowStream' >> beam.Map(lambda k,v: rowToBQ.parse_method_stream(k,v))
          | 'WriteToBigQueryStream' >> beam.io.WriteToBigQuery(
                table = known_args.output_s,
                schema = rowToBQ.stream_schema))

    # write message to pubsub with a different output_topic 
    # for users to subscribe to and retrieve real time analysis data
    # (avgs | 'PublishToPubSub' >> )
    
    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
