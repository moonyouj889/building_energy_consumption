# for python2
import dateutil.parser


def iso_to_datetime(timestamp):
    # for python2
    return dateutil.parser.parse(timestamp)