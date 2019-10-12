import logging
import json
import csv
import argparse
import os
import datetime

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.pipeline import PipelineOptions
from apache_beam.transforms.combiners import Mean



# data gets collected 4 times per hour (every 15 minutes)
DATA_COLLECTION_FREQUENCY = 4
ROWS_PER_DAY = 10
# ROWS_PER_DAY = DATA_COLLECTION_FREQUENCY * 24
SCHEMA_PATH = 'data/processed_data/bq_schemas.txt'
# WINDOW_SIZE = 3600
# WINDOW_PERIOD = 900
WINDOW_SIZE = 60
WINDOW_PERIOD = 15

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
        logging.info('passed Row: {}'.format(row))
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
        datetimeNow = str(datetime.datetime.utcnow())
        logging.info('printing datetime {}'.format(datetime.datetime.utcnow()))
        logging.info('printing datetime in proper BQ format {}'.format(datetimeNow))
        row = {'timestamp': datetimeNow,
                'building_id': int(k),
                'Gen_Avg': int(v)}
        logging.info('passed Row for Streams: {}'.format(row))
        return row

x = BQTranslateTransformation()
print(x.parse_method_stream('3', '453'))