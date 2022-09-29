import pytz

import dateutil.parser as DateParser


class DatetimeUtil(object):
    '''Utility functions for datetimes'''

    @staticmethod
    def toRfc3339(dt):
        '''
        Convert a UTC datetime to the RFC-3339 format.
        See: https://stackoverflow.com/questions/8556398/generate-rfc-3339-timestamp-in-python
        '''
        dt = dt.replace(tzinfo=pytz.UTC)
        return dt.isoformat()
    
    @staticmethod
    def fromRfc3339(dt):
        '''Convert a UTC datetime from the RFC-3339 format.'''
        return DateParser.parse(dt)
