from enum import Enum


class L2aStatus(Enum):
    '''L2A product status.'''
    
    # Lower-case, as in the database
    pending = 1
    generated = 2
    generation_aborted = 3
    deleted = 4
