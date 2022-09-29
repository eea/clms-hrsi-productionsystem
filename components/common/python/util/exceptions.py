class CsiExternalError(Exception):
    """
    Raised when an error occured from an external CSI dependencie, like an HTTP
    endpoint or a copy from/to a bucket. This error means that maybe it won't
    happen again if we try again.
    """
    def __init__(self, subtype, message):
        self.subtype = subtype
        self.message = message

class CsiInternalError(Exception):
    """
    Raised when an error occured due to a problem from the CSI system. This error
    means that it is probabably a bug and the error will happen again if we try
    again.
    """
    def __init__(self, subtype, message):
        self.subtype = subtype
        self.message = message

class TimeoutException(Exception):
    """ Simple Exception to be called on timeouts. """
    pass