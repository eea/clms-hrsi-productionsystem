from enum import Enum


class AssemblyStatus(Enum):
    '''Sentinel-1 Assembly status.'''

    # Lower-case, as in the database
    pending = 1
    generated = 2
    empty = 3
    generation_aborted = 4
    deleted = 5
