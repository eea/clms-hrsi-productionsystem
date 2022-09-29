
class ForeignAttribute(object):
    '''Attribute linked by a foreign key.'''

    def __init__(self, foreign_id, foreign_table, _type, name):

        # Foreign key ID in the database, e.g. fk_foreign_id int references foreign_table
        self.foreign_id = foreign_id
        
        # Foreign table in the database, e.g. foreign_table
        self.foreign_table = foreign_table

        # Foreign attribute Python type, e.g. ForeignObject
        self.type = _type

        # Foreign attribute name in the Python class, e.g. foreign_object
        self.name = name
