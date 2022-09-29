from .rest_database import RestDatabase
from ...util.exceptions import CsiInternalError


class ForeignKey(RestDatabase):
    '''
    Must be implemented by child classes to manage attributes linked by foreign keys.
    We override the __getattr__ and __setattr__ functions to simulate the fact that
    the current class would be a child class of the foreign attribute.

    :param [fk_foreign_id]: ([str]) list of foreign key IDs (names configurable)
    :param [foreign_object]: ([object]) list of foreign objects (names configurable)
    :param foreign_attr: ([ForeignAttribute]) foreign attributes.
    '''

    def __init__(self, table_name, foreign_attr_values, id_name=None):
        '''
        :param foreign_attr_values: (dict {ForeignAttribute: existing_value}) foreign attributes 
        with their existing values (or None to create a new value from the default constructor).
        '''

        # For each foreign attribute: self.fk_foreign_id = None
        for attr in foreign_attr_values.keys():
            self.set_foreign_id(attr, None)

        # Call the parent constructor AFTER all the attributes are initialized
        super().__init__(table_name, id_name)

        # Save the foreign attributes
        self.foreign_attrs = list(foreign_attr_values.keys())

        # For each foreign attribute: self.foreign_object = value
        # Or if value is None: self.foreign_object = ForeignObject()
        for attr, value in foreign_attr_values.items():
            self.set_foreign_object(attr, value if (value is not None) else attr.type())

    def set_foreign_id(self, foreign_attr, id_value):
        '''Set foreign attribute ID value, e.g. self.fk_foreign_id = 1'''

        # Use this notation to avoid recursion
        object.__setattr__(self, foreign_attr.foreign_id, id_value)

    def get_foreign_id(self, foreign_attr):
        '''Get foreign attribute ID value, e.g. self.fk_foreign_id'''

        # Use this notation to avoid recursion
        return object.__getattribute__(self, foreign_attr.foreign_id)

    def set_foreign_object(self, foreign_attr, value):
        '''Set foreign object, e.g. self.foreign_object = ForeignObject()'''

        # Use this notation to avoid recursion
        object.__setattr__(self, foreign_attr.name, value)

        # Also update this foreign attribute ID.
        if not isinstance(value, RestDatabase):
            raise Exception(
                'Foreign objects must implement %s' %
                RestDatabase.__name__)  # @UndefinedVariable
        self.set_foreign_id(foreign_attr, value.get_id())

    def get_foreign_object(self, foreign_attr):
        '''Get foreign object, e.g. self.foreign_object'''

        # Use this notation to avoid recursion
        return object.__getattribute__(self, foreign_attr.name)

    def __getattr__(self, name):
        '''
        Simulate the fact that the current class would be a child class of the foreign attributes.
        This function is called only when the attribute does not exist in the current object. 
        '''

        # Else, if the attribute exists in a foreign attribute, return its value.
        # It is used as a shortcut by the caller to call e.g. return child.attr_name
        # instead of return child.foreign_object.attr_name
        try:

            # Use this notation to avoid recursion
            for foreign_attr in object.__getattribute__(self, 'foreign_attrs'):

                # Get the foreign object
                foreign_object = self.get_foreign_object(foreign_attr)

                # If the attribute exist in the current object, return its value
                try:
                    return getattr(foreign_object, name)
                except Exception:
                    pass
        except Exception:
            pass

        # Else call the standard method, it will raise an error.
        return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        '''
        Simulate the fact that the current class would be a child class of the foreign attributes.
        Works in the same way as __getattr__
        '''

        # If the attribute exists in the current object, set its value.
        # Use this notation to avoid recursion.
        try:
            object.__getattribute__(self, name)
            return object.__setattr__(self, name, value)
        except Exception:
            pass

        # Else, if the attribute exists in a foreign attribute, set its value.
        try:
            for attr in object.__getattribute__(self, 'foreign_attrs'):

                # Get the foreign object
                foreign_object = self.get_foreign_object(attr)

                # If the attribute exist in the current object, set its value
                if hasattr(foreign_object, name):
                    return setattr(foreign_object, name, value)

        except Exception:
            pass

        # Else call the standard method
        return object.__setattr__(self, name, value)

    def get(self, logger_func=None):
        '''Find objects from the database.'''

        # Construct the select query. See: http://postgrest.org/en/v6.0/api.html#embedded-filters
        # e.g. GET /self_table_name?select=*,foreign_table(*)
        if not 'select' in self.query_params:
            select = '*'
            for attr in self.foreign_attrs:
                select += ',%s(*)' % attr.foreign_table
            self.query_params['select'] = select

        # Call the parent method
        return super().get(foreign_attributes=self.foreign_attrs, logger_func=logger_func)

    def post(self, post_foreign, logger_func=None):
        '''
        Insert this object into the database.

        :param post_foreign: automatically insert the foreign attributes. TODO: maybe the caller
        should be able to choose which foreign attributes to insert or not.
        '''
        try:
            # Post the foreign attributes first
            if post_foreign:
                for attr in self.foreign_attrs:

                    # Get the foreign object
                    foreign_object = self.get_foreign_object(attr)

                    # Post it
                    foreign_object.post(logger_func)

                    # Update the corresponding ID in the current object,
                    # e.g. self.fk_foreign_id = foreign_object.id
                    foreign_id_value = foreign_object.get_id()
                    self.set_foreign_id(attr, foreign_id_value)

            # Post the current object (call the parent method, not the current one)
            response = super().post(logger_func)
            return response
            
        except CsiInternalError as exception:
            raise exception

        except Exception as exception:
            if post_foreign:
                raise Exception(
                    'Error posting interdependent entries. Consider using rollback or '
                    'see http://postgrest.org/en/v6.0/api.html#stored-procedures') from exception
            else:
                raise

    def patch(self, patch_foreign, logger_func=None):
        '''Patch (update) an object from the database.

        :param post_foreign: automatically insert the foreign attributes.
        '''
        try:
            # Patch the foreign attributes first
            if patch_foreign:
                for attr in self.foreign_attrs:
                    foreign_object = self.get_foreign_object(attr)
                    foreign_object.patch(logger_func)

            # Patch the current object (call the parent method, not the current one)
            super().patch(logger_func)

        except CsiInternalError as exception:
            raise exception

        except Exception as exception:
            if patch_foreign:
                raise Exception([exception, Exception(
                    'Error patching interdependent entries. Consider using rollback or '
                    'see http://postgrest.org/en/v6.0/api.html#stored-procedures')])
            else:
                raise