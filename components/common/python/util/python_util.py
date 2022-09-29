
class PythonUtil(object):
    '''Python utility functions.'''
    
    @staticmethod
    def is_list(obj):
        '''Return True if an object is a list [1, 2, ...], tuple (1, 2, ...) or set {1, 2, ...}'''
        return (obj != None) and isinstance(obj, (list, tuple, set))
    
    @staticmethod
    def ensure_in_list(obj, convert_none):
        '''
        Returns the input object in a list if it is not already in a list, tuple or set.
        If it is in a tuple or set, convert it to a list.
        :param convert_none: If input is None, convert it to [None] or not ?
        '''
        if (obj == None) and (not convert_none):
            return None
        elif not PythonUtil.is_list(obj):
            return [obj]
        elif isinstance(obj, (tuple, set)):
            return list(obj)
        else:
            return obj
