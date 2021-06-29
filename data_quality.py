import argparse
import logging
import re
import datetime
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class DataIngestion:

    # method detect null values and replace with word error
    def parse_report_null_method(self, string_input,filename):
        # The row will have a 2 fields
        # timestamp and the row with the word Error in the null field
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        # Strip out carriage return, newline and quote characters.
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',string_input)))
        # values have the structure: a,b,c,d,,f
        # Filter and replace null for Error
        result = list(map(lambda a: str(a).replace("",'Error') if len(a)==0 else a,values))
        # Result is a list that have ( 'a', 'b' , 'c', 'd', 'Error' , 'f' )
        result = ','.join([str(elem) for elem in result])
        #we create again the values 
        values = [timestamp,result,filename]

        dictionary_val = ['date_timestamp','data','filename']
        # Now result is a string that have a,b,c,d,Error,f
        # It is neccesary because we need to detect all possible errors in the row
        # they could be at the beginning, in between or at the end so we can check it only
        # with the length of the field

        row = dict(zip(dictionary_val,values))
        return row
        
def run(argv=None):

    parser = argparse.ArgumentParser()
    # The input file to read and the output table to write.
    parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input file to read. This can be a local file or '
                        'a file in a Google Storage Bucket.')

    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output BQ table to write results to.')
    
    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # DataIngestion is a class that hold the logic 
    data_ingestion = DataIngestion()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
        # Read the file. This is the source of the pipeline. 
        | 'Read from a File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
        # We need to check if there are null values in the rows 
        # This function will be run in parallel on different workers using input from the
        # previous stage of the pipeline
        | 'Detect Errors' >> beam.Map(lambda s: data_ingestion.parse_report_null_method(s,known_args.input))
        # Filter the Errors detected     
        | 'Filter Errors' >> beam.Filter(lambda s: 'Error' in s['data'] )
        # Write the errors detected in a error table in BigQuery
        | 'Write Errors in BigQuery' >> beam.io.Write(
            beam.io.BigQuerySink(
            # The table name 
            known_args.output,
            # Here we use the simplest way of defining a schema:
            # fieldName:fieldType
            schema='date_timestamp:TIMESTAMP,data:STRING,filename:STRING',
            # Creates the table in BigQuery if it does not yet exist.
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
