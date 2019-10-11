import csv
from datetime import datetime
import gzip
import os
import shutil
import logging
import json


DATA_FOLDER = './data/processed_data/'
INPUT = 'buildings-energy-consumption-clean-data.csv'
OUTPUT = DATA_FOLDER + 'chrono-comma-sep-' + INPUT
FIELD_CLEANED_OUTPUT = DATA_FOLDER + 'data-to-pubsub-' + INPUT
SCHEMA_OUTPUT = DATA_FOLDER + 'bq_schemas.txt'
FIELDNAMES_OUTPUT = DATA_FOLDER + 'fieldNames.txt'

# takes in header from original csv and prints them for user to visualize
# also writes the json of schema to be saved in a separate file
# to be used for Big Query for the ease of analysts querying
'''
load schema format:
{'fields': [
    {'name':'timestamp',
     'type':'TIMESTAMP',
     'mode':'REQUIRED'
    }, {...}
]}
where everything except the timestamps are int type and nullable

! Stream schema format is the same as load schema,
  except that the timestamp represents the timestamp of the start of the window of the avg

original column format:
    - for general meter: [buildingNum]_Main Meter_Active energy
    - for sub meter: [buildingNum]_Sub Meter Id[idNum]_Active energy
column format to change to:
    [buildingNum]_[meterType]_[idNum]
        1-8          'Gen'     1-N (no suffix for 'Gen')
                  or 'Sub'
    
    ex) general meter for building 7: 7_Gen
        submeter 3 for building 2: 2_Sub_3
'''
def translate_fieldNames(fieldNames):
    buildingNum = ''
    # initialize schemas and field names since timestamp exists in all
    schemas = [{}, {}, {}, {}, {}, {}, {}, {}]
    for schema in schemas: 
        schema['fields'] = []
        schema['fields'].append({'name': 'timestamp',
                                 'type': 'TIMESTAMP',
                                 'mode': 'REQUIRED'})
        schema['fields'].append({'name': 'building_id',
                                 'type': 'INTEGER',
                                 'mode': 'REQUIRED'})
    field_names = ['timestamp']

    for field in fieldNames:
        # if field is "Timestamp", continue to nxt iteration
        if field == "Timestamp": continue
        
        if buildingNum != field[0] and buildingNum != '':
            logging.info('')
        buildingNum = int(field[0])

        meterTypeIndicator = field[2]
        schema = schemas[buildingNum-1]
        # in case new fields get added, add them manually in dataflow file
        if meterTypeIndicator == 'M' or meterTypeIndicator == 'S': 
            # handle general meter
            fieldType = 'FLOAT'
            fieldMode = 'NULLABLE'
            if meterTypeIndicator == 'M':
                meterType = 'Gen'
                fieldName = '{}_{}'.format(meterType, buildingNum)
            # handle sub meter
            else:
                meterType = 'Sub'
                idNum = ''
                idStart = 14
                while field[idStart].isnumeric():
                    idNum += field[idStart]
                    idStart += 1
                fieldName = '{}_{}_{}'.format(meterType, buildingNum, idNum)

            schema['fields'].append({'name': fieldName,
                                     'type': fieldType,
                                     'mode': fieldMode})
        field_names.append(fieldName)
        logging.info('{} --> {}'.format(field, fieldName))

    logging.info('')
    return (schemas, field_names)


if __name__ == '__main__':
    logging.basicConfig(
        format='%(message)s', level=logging.INFO)

    try:
        shutil.rmtree(DATA_FOLDER)
        # csvPath = os.path.abspath(OUTPUT)
        # os.unlink(csvPath)
        logging.info("INFO: Old processed csv found and removed.")
    except:
        logging.info(
            "INFO: No previously processed data found.")

    logging.info("Continuing csv processing...")
    logging.info("\n_______FIELD NAMES_______")

    # sort the order of ther rows by timestamps
    with open('./data/' + INPUT, 'r') as jumbled_data:
        jumbled_rows = csv.DictReader(jumbled_data, delimiter=';')
        bq_schemas, fieldNames = translate_fieldNames(jumbled_rows.fieldnames)
        jumbled_rows = csv.DictReader(jumbled_data, fieldnames=fieldNames, delimiter=';')
        chrono_data = sorted(jumbled_rows, key=lambda row: (row['timestamp']))

    if not os.path.exists(DATA_FOLDER):
        os.mkdir(DATA_FOLDER)
        logging.info(
            "INFO: processed_data directory recreated.")

    with open(FIELDNAMES_OUTPUT, 'w') as fieldNames_file:
        splitNames = []
        curr_fields = []
        curr_building_id = 1
        for field in fieldNames[1:]:
            if field[4] != curr_building_id:
                if len(curr_fields) > 0: splitNames.append(curr_fields)
                curr_fields = []
                curr_building_id = field[4]
            curr_fields.append(field)
        json.dump(splitNames, fieldNames_file)
        logging.info(
            "INFO: fieldNames.txt created.")

    with open(SCHEMA_OUTPUT, 'w') as schema_file:
        json.dump(bq_schemas, schema_file)
        logging.info(
            "INFO: bq_schemas.txt created.")

    with open(OUTPUT, 'w') as chronological_data:
        chronological_rows = csv.DictWriter(chronological_data, 
                                            fieldnames=fieldNames, 
                                            delimiter=',')
        chronological_rows.writeheader()
        chronological_rows.writerows(chrono_data)
        logging.info("INFO: chronological data created.")

    with open(OUTPUT, 'rb') as f_in, gzip.open(OUTPUT + '.gz', 'wb') as f_out:
        f_out.writelines(f_in)
        logging.info(
            "INFO: gzipFile of chronological data created.")
