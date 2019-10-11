def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    # Here we add some specific command line arguments we expect.   Specifically
    # we have the input file to load and the output table to write to.
    parser.add_argument(
        '--input', dest='input', required=False,
        help='Input file to read.  This can be a local file or '
             'a file in a Google Storage Bucket.',
        # This example file contains a total of only 10 lines.
        # It is useful for developing on a small set of data
        default='gs://python-dataflow-example/data_files/head_usa_names.csv')
    # This defaults to the temp dataset in your BigQuery project.  You'll have
    # to create the temp dataset yourself using bq mk temp
    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='lake.usa_names_transformed')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)
    # DataTransformation is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataTransformation()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line.  This includes information like where Dataflow should
    # store temp files, and what the project id is.
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    schema = parse_table_schema_from_json(data_ingestion.schema_str)

    (p
     # Read the file.  This is the source of the pipeline.  All further
     # processing starts with lines read from the file.  We use the input
     # argument from the command line.  We also skip the first line which is a
     # header row.
     | 'Read From Text' >> beam.io.ReadFromText(known_args.input,
                                                skip_header_lines=1)
     # This stage of the pipeline translates from a CSV file single row
     # input as a string, to a dictionary object consumable by BigQuery.
     # It refers to a function we have written.  This function will
     # be run in parallel on different workers using input from the
     # previous stage of the pipeline.
     | 'String to BigQuery Row' >> beam.Map(lambda s:
                                            data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            known_args.output,
            # Here we use the JSON schema read in from a JSON file.
            # Specifying the schema allows the API to create the table correctly if it does not yet exist.
            schema=schema,
            # Creates the table in BigQuery if it does not yet exist.
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
© 2019 GitHub, Inc.