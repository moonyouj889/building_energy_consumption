from processCSV import print_fieldNames
import logging
import numpy as np
from random import randint
import gzip
import csv

# In this scenario, it was assumed that
# the IoT Gateway aggregated the data separately generated from 8 buildings
# to a single event data prior to publishing to Pub/Sub.
# Original data was missing submeter vals
# (e.g. submeter of building 1 has id1 and id14, but none inbetween),
# so the missing columns were filled in randomly making sure that
# the sum of all submeter vals were no greater than general meter val

DATA_FOLDER = './data/processed_data/'
INPUT = DATA_FOLDER + 'chrono-comma-sep-buildings-energy-consumption-clean-data.csv'
OUTPUT = DATA_FOLDER + 'filled-' + INPUT
GZ_OUTPUT = OUTPUT + '.gz'


if __name__ == '__main__':
    logging.basicConfig(
        format='%(message)s', level=logging.INFO)

    with open('./data/' + INPUT, 'r') as jumbled_data:
        jumbled_rows = csv.DictReader(jumbled_data, delimiter=';')
        print_fieldNames(jumbled_rows.fieldnames)
        chrono_data = sorted(jumbled_rows, key=lambda row: (row['Timestamp']))

    with open(OUTPUT, 'w') as chronological_data:
        chronological_rows = csv.DictWriter(
            chronological_data, fieldnames=jumbled_rows.fieldnames, delimiter=',')
        chronological_rows.writeheader()
        chronological_rows.writerows(chrono_data)

    with open(OUTPUT, 'rb') as f_in, gzip.open(GZ_OUTPUT, 'wb') as f_out:
        f_out.writelines(f_in)
