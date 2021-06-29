import argparse
import logging
import re
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class DataIngestion:

    def parse_method(self, string_input, dictionary):
 
        # String that have the dictionary
        dictionary_val = re.split(",", re.sub('\r\n', '', re.sub('"', '',dictionary)))
        # Strip out carriage return, newline and quote characters.
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',string_input)))
        row = dict(zip(dictionary_val,values))   
        return row
    
def run(argv=None):
    parser = argparse.ArgumentParser()
    # Specific command line arguments we expect.
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input file to read.')

    # This defaults to the lake dataset in your BigQuery project. You'll have
    # to create the lake dataset yourself using this command:
    # bq mk lake
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output BQ table to write results to.',
                        default='lake.f_event_sessions')
    
    # schema
    parser.add_argument('--schema',
                        dest='schema',
                        required=True,
                        help='schema of the dataset fieldName:fieldType, example: name:STRING,date_register:DATE,code:NUMERIC',
                        default='profileId:STRING,dt:DATE,medium:STRING,eventCategory:STRING,landinpagepath:STRING,country:STRING,sessions:NUMERIC')

    # dictionary
    parser.add_argument('--dictionary',
                        dest='dictionary',
                        required=True,
                        help="data dictionary, example ('state','gender')",
                        default="'profileId','dt','medium','eventCategory','landinpagepath','country','sessions','users'")

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # DataIngestion is a class that hold the logic 
    # transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p

     | 'Read from a File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
     # This function will be run in parallel on different workers using input from the previous stage of the pipeline.
     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s,known_args.dictionary)) 
     # Write the data into BigQuery
     | 'Write to BigQuery' >> beam.io.Write(
         beam.io.BigQuerySink(
             # The table name 
             known_args.output,
             # The schema syntax: fieldName:fieldType
             schema=known_args.schema,
             # Creates the table in BigQuery if it does not yet exist.
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             # Append data in the BigQuery table.
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
             
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
