from apache_beam.options.pipeline_options import GoogleCloudOptions
import argparse
import os
import apache_beam as beam


class MyOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input_topic',
            help='Pubsub topic for the input stream',
            default='energy',
            dest='input_topic')
        parser.add_argument(
            '--bq_output',
            help='BQ table destimation of the output stream',
            default='building-energy-consumption:buildings.energy',
            dest='bq_out')


def run(argv=None):
    options = MyOptions()
    known_args, pipeline_args = parser.parse_known_args(argv)


if __name__ == '__main__':
    print('')
    run()
