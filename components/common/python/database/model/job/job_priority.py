from ....util.ordered_enum import OrderedEnum


class JobPriority(OrderedEnum):
    '''    
    Each CoSIMS job is attributed a priority level that affects its delay.
    In the Near Real-Time context:
        * The NRT level is the highest priority with timeliness criticality.
        * The DELAYED level is used for late input products publication.
    The REPROCESSING level is used in the archive reprocessing, PSA and ARLIE contexts.
    '''
    
    # Lower-case, as in the database.
    # The lower the number, the higher the priority, e.g.
    # nrt=1 has higher priority than delayed=2, which has
    # higher priority than reprocessing=3.    
    nrt = 1
    delayed = 2
    reprocessing = 3
