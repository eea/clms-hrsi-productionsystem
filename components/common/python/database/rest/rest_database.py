'''
_summary_

:raises Exception: _description_
:raises Exception: _description_
:raises CsiInternalError: _description_
:return: _description_
:rtype: _type_
'''

import datetime
from enum import Enum
import json
import os
import urllib3
from requests import exceptions

from ...util.datetime_util import DatetimeUtil
from ...util.python_util import PythonUtil
from ...util.rest_util import RestUtil
from ...util.sys_util import SysUtil
from ...util.log_util import temp_logger
from ...util.exceptions import CsiInternalError

class RestDatabase():
    '''
    Must be inherited by child classes to perform automatic REST operations.
    Uses PostgREST, see: http://postgrest.org/en/v6.0/api.html

    :param id: Unique entry ID in the database. Must be a 'serial primary key'.
    The name 'id' is configurable.
    :param id_name: Configured ID name.
    :param uri: URI for the table associated with the child class.
    :param fields: database table fields.
    '''

    # URI root containing the database host and port.
    # COSIMS_DB_HTTP_API_BASE_URL is e.g. http://localhost:3000
    # URI_ROOT becomes e.g. http://localhost:3000/%s
    URI_ROOT = '%s/%%s' % SysUtil.read_env_var('COSIMS_DB_HTTP_API_BASE_URL')

    # From the PostgREST doc http://postgrest.org/en/v6.0/api.html#insertions-updates
    # The response will include a Location header describing where to find the new object.
    # You can get the full created object back in the response to your request by including
    # the header Prefer: return=representation.
    HEADERS_RETURN_REPRESENTATION = {'Prefer': 'return=representation'}

    # Environment variable used to set url leading to database
    __DATABASE_URL_ENV_VAR = "COSIMS_DB_HTTP_API_BASE_URL"

    # Error message to be raised when we can't reach the data base
    __DATABASE_ERROR = "Unable to establish connection with database."

    # Error message to be raised when the database content has been reset
    #  while services were running
    __DATABASE_CONTENT_ERROR = "Inconsistent content between database " \
                               "and services, services should be restarted."

    # Error message to be raised when a new job is attempted to be added to
    # the database, but it violate unicity constraint
    __DATABASE_UNIQUE_KEY_ERROR = "New entry can't be added to database as " \
                                  "it violate the unique constraint established on this entry type."

    # Error message to be raised when a non-valid job status transition is requested
    __JOB_STATUS_TRANSITION_ERROR_MESSAGE = "Non-valid job status transition " \
                                            "requested, stopping services."

    # Error message to be raised when a non-valid job name is set
    __INVALID_JOB_NAME_ERROR_MESSAGE = "Non-valid job name is attempted to be " \
                                       "set , stopping services."

    HTTP_CRITICAL_ERROR = [400, 409, 429]
    # Counter to log only one error message
    __DATABASE_PREVIOUSLY_IN_ERROR = False
    __DATABASE_INCONSISTENT_CONTENT = False


    def __init__(self, table_name='', id_name=None):

        # Default ID name = 'id'
        if not id_name:
            id_name = 'id'

        # Init the database ID to None
        setattr(self, id_name, None)

        # Save the existing class attributes: they correspond to the database table fields.
        self.fields = list(self.__dict__.keys())

        # Save the ID name
        self.id_name = id_name

        # Compose the full URI
        self.table_name = table_name
        self.uri = RestDatabase.URI_ROOT % self.table_name

        # Build a string for PostgREST query parameters as a dict,
        # e.g. status=eq.initialized AND tile_id=eq.32TLR
        self.query_params = {}

    def get_id(self):
        '''Get the database ID value: return self.id'''
        return getattr(self, self.id_name)

    def set_id(self, value):
        '''Set the database ID value: self.id = value'''
        return setattr(self, self.id_name, value)

    def to_database_value(self, _, value):
        '''Return a value as it must be inserted in the database.'''

        # Return enum name
        if isinstance(value, Enum):
            return value.name

        # Convert datetimes to string
        elif isinstance(value, datetime.datetime):
            return DatetimeUtil.toRfc3339(value)

        # Convert dictionaries and list to json
        elif isinstance(value, dict) or isinstance(value, list):
            return json.dumps(value)

        # Return default value
        return value

    def from_database_value(self, _, value):
        '''
        Parse a string value as it was inserted in the database.
        To be implemented by child classes.
        '''
        # By default : return string value
        return value

    @staticmethod
    def parse_response(response, object_instance, foreign_attrs=None):
        '''Return the objects contained in a REST response.'''

        # Each element in the JSON response is a new object.
        # Parse and return them.
        objects = []
        # if response is not None:
        if response is not None and response.json is not None:
            for dict_response in response.json():

                # New object of the same type as the instance given as example.
                # Call the default constructor.
                try:
                    new_object = type(object_instance)()
                except Exception as exception:
                    raise Exception(f'A default constructor must exist for type \
                          {type(object_instance).__name__}') from exception

                try:

                    # Fill the new object from the JSON response
                    RestDatabase.__parse_response_object(dict_response, new_object)

                    # Fill the foreign attributes
                    if foreign_attrs:
                        for foreign_attr in foreign_attrs:

                            # If the foreign table is missing from the JSON response, it means
                            # that it was not requested.
                            if not foreign_attr.foreign_table in dict_response:
                                continue  

                            # Foreign fields in the JSON response
                            foreign_dict = dict_response[foreign_attr.foreign_table]

                            # If the foreign dict is None, it means that a a filter was applied
                            # on the foreign object and it returned None. The whole object must
                            # not be saved.
                            # See http://postgrest.org/en/v6.0/api.html#embedded-filters
                            # "Once again, this restricts the roles included to certain characters
                            # but does not filter the films in any way. Films without any of those
                            # characters would be included along with empty character lists.
                            if foreign_dict is None:
                                raise Exception

                            # Foreign instance in the new object
                            foreign_instance = getattr(new_object, foreign_attr.name)

                            # Fill the foreign attributes from the JSON response
                            RestDatabase.__parse_response_object(foreign_dict, foreign_instance)

                    # Save the new object
                    objects.append(new_object)

                # In case of error, if the new object or at least one foreign object is missing
                # from the response, do not save the new object.
                except Exception:
                    pass

        return objects

    @staticmethod
    def __parse_response_object(dict_response, new_object):
        '''Fill object attributes from JSON response.'''

        # Parse each registered key.
        # Set attribute to None if the key doesn't exist in the response.
        for attribute in new_object.fields:
            try:
                value = new_object.from_database_value(attribute, dict_response[attribute])
            except Exception:
                value = None
            setattr(new_object, attribute, value)

    def __to_post_data(self):
        '''
        Create the POST data by keeping only the original attributes
        from the current instance.
        '''

        # POST data, as a dict
        data = {}

        # For each attribute of the current object
        for attribute, value in self.__dict__.items():

            # If it is registered
            if attribute in self.fields:

                # Get and convert its value
                data[attribute] = self.to_database_value(attribute, value)

        return data

    def catch_errors(self, exception, logger_func=None):
        '''
        Catch the error raised by the requests sent, and reformat them into 
        human readable versions.
        '''

        exception_logger = logger_func if logger_func else temp_logger.error
        if isinstance(exception, (exceptions.ConnectionError, exceptions.HTTPError, urllib3.exceptions.NewConnectionError)):
            # Error linked to inconsistency in DataBase content,
            #  requiring to restart services
            if isinstance(exception, exceptions.HTTPError) and (exception.response.status_code in self.HTTP_CRITICAL_ERROR):

                # Parse error message content
                error_content = exception.response.content.decode('ascii')

                error_subtype = "Unknown Communication Error"
                error_message = error_content

                if not self.__DATABASE_INCONSISTENT_CONTENT:
                    # Display one error message if non valid job status transition
                    #  is requested

                    # Error raised by database triggers
                    if exception.response.status_code == 400:
                        # non valid job status transition has been requested
                        if 'invalid job status transition' in error_content:
                            exception_logger("%s : %s", self.__JOB_STATUS_TRANSITION_ERROR_MESSAGE, exception)
                            error_subtype = "Job Status Transition Error"
                            error_message = f"{self.__JOB_STATUS_TRANSITION_ERROR_MESSAGE} " \
                                            f"Error : {error_content}"

                        # non valid job name has been set
                        else:
                            exception_logger("%s : %s", self.__INVALID_JOB_NAME_ERROR_MESSAGE, exception)
                            error_subtype = "Invalid Job Name Error"
                            error_message = f"{self.__INVALID_JOB_NAME_ERROR_MESSAGE} " \
                                            f"Error : {error_content}"

                    # Display one error message if database content is inconsistent
                    elif exception.response.status_code == 409:
                        # duplicate key value violates unique constraint
                        if 'duplicate key value violates unique constraint' in error_content:
                            exception_logger("%s : %s", self.__DATABASE_UNIQUE_KEY_ERROR, exception)
                            error_subtype = "Duplicate Key Value Violates Unique Constraint"
                            error_message = f"{self.__DATABASE_UNIQUE_KEY_ERROR} " \
                                            f"Error : {error_content}"

                        else:
                            exception_logger("%s : %s", self.__DATABASE_CONTENT_ERROR, exception)
                            error_subtype = "Database Content Inconsistency"
                            error_message = f"{self.__DATABASE_CONTENT_ERROR} " \
                                            f"Error : {error_content}"

                    # Notify that database encountered a content issue
                    self.__DATABASE_INCONSISTENT_CONTENT = True

                # Raise exception to stop services
                raise CsiInternalError(
                    error_subtype,
                    error_message
                ) from exception

            # Display only one error message if database is down
            if not self.__DATABASE_PREVIOUSLY_IN_ERROR:
                exception_logger("%s : %s", self.__DATABASE_ERROR, exception)
                self.__DATABASE_PREVIOUSLY_IN_ERROR = True

            return None

        else:
            # Display other error messages
            exception_logger("%s", exception)
            return None

    def get(self, foreign_attributes=None, logger_func=None, parse_response=True):
        '''
        Find objects from the database.

        :param foreign_attributes: also parse attributes that are linked by a foreign key.
        '''

        database_url = os.environ[self.__DATABASE_URL_ENV_VAR]

        response = None
        url = None

        # Send a Get request
        try:
            response, url = RestUtil().get(
                url=self.uri,
                params=self.query_params,
                logger_func=logger_func,
                return_url=True)

            if self.__DATABASE_PREVIOUSLY_IN_ERROR and (database_url in url):
                self.__DATABASE_PREVIOUSLY_IN_ERROR = False
                logger_func("Database communication back to nominal state!")

        except Exception as exception:
            self.catch_errors(exception, logger_func=logger_func)

        # Parse objects contained in the response
        if parse_response:
            return RestDatabase.parse_response(response, self, foreign_attributes)
        
        return response

    def post(self, logger_func=None, data=None, parse_response=True):
        '''Insert this object into the database.'''

        database_url = os.environ[self.__DATABASE_URL_ENV_VAR]

        # The database ID attribute should not exist.
        # It is automatically attributed by the database.
#         if hasattr(self, self.id_name):
#             raise Exception('The database ID attribute should not exist.')

        # Insert a new entry with all this object attributes.
        # Get the inserted data in return.

        if data is None:
            data = self.__to_post_data()

        response = None
        url = None

        try:
            response, url = RestUtil().post(
                url=self.uri,
                headers=RestDatabase.HEADERS_RETURN_REPRESENTATION,
                params=None,
                data=data,
                logger_func=logger_func,
                return_url=True)

            if self.__DATABASE_PREVIOUSLY_IN_ERROR and (database_url in url):
                self.__DATABASE_PREVIOUSLY_IN_ERROR = False
                logger_func("Database communication back to nominal state!")

        except Exception as exception:
            self.catch_errors(exception, logger_func=logger_func)

        if parse_response:
            # Used to get the inserted id : define a class with a single
            # field = this class database ID attribute.
            id_name = self.id_name
            fields_id = [self.id_name]
            class ParseId(RestDatabase):
                '''
                _summary_

                :param RestDatabase: _description_
                :type RestDatabase: _type_
                '''
                def __init__(self):
                    self.id_name = id_name
                    self.fields = fields_id
            
            parse_id = ParseId()

            if response is None:
                return None

            # Get the inserted ID
            inserted = RestDatabase.parse_response(response, parse_id)[0]
            id_value = inserted.get_id()
            self.set_id(id_value)

            if logger_func:
                logger_func(f'Attributed serial ID: {id_value}')

            return inserted
        
        return response

#     @staticmethod
#     def bulk_post(objects, logger_func=None):
#         '''
#         Bulk insert objects (=insert all objects with one call) into database.
#         See: http://postgrest.org/en/v6.0/api.html#bulk-insert
#         '''
#
#         # At least one object must be passed
#         if not objects:
#             return
#
#         # All the inserted objects must be of the same type
#         types = set([str(type(_object)) for _object in objects])
#         if len(types) != 1:
#             raise Exception(
#                 'All objects to bulk insert must be of the same type:\n - %s' %
#                 '\n - '.join(types))
#
#         # Object instance example = first one of the list
#         object_instance = objects[0]
#
#         # Create a JSON array of objects to insert
#         data = json.dumps([_object.__to_post_data() for _object in objects])
#
#         # Insert the new records.
#         # Get the inserted data in return.
#         response = RestUtil().post(
#             url=object_instance.uri,
#             headers=RestDatabase.HEADERS_RETURN_REPRESENTATION,
#             params=None,
#             data=data,
#             logger_func=logger_func)
#
#         # Note: does not work with auto-generated IDs (they are None before insertion)
#         # Maybe we could try to insert into a view that copies the table columns but without the ID
#         # + we should parse the response to attribute the auto-generated IDs to the input objects.

    def patch(self, logger_func=None, data=None, params=None):
        '''Patch (update) an object from the database.'''

        database_url = os.environ[self.__DATABASE_URL_ENV_VAR]

        if data is None:
            data = self.__to_post_data()
        if params is None:
            params={self.id_name: 'eq.%s' % self.get_id()}

        url = None

        # Patch the current object, identified by its unique ID
        try:
            _, url = RestUtil().patch(
                url=self.uri,
                params=params,
                data=data,
                logger_func=logger_func,
                return_url=True)

            if self.__DATABASE_PREVIOUSLY_IN_ERROR and (database_url in url):
                self.__DATABASE_PREVIOUSLY_IN_ERROR = False
                logger_func("Database communication back to nominal state!")

        except Exception as exception:
            self.catch_errors(exception, logger_func=logger_func)

    #
    # Build a string for PostgREST query parameters.

    def select(self, attribute_names):
        '''
        select=name1,name2
        :param names: one attribute name (str), or a list of [attribute names]. 
        '''
        self.query_params['select'] = (
            ','.join(attribute_names) 
            if PythonUtil.is_list(attribute_names) 
            else attribute_names) 
        return self

    def attribute_eq(self, attribute_name, value):
        '''name=eq.value'''
        self.query_params[attribute_name] = 'eq.%s' % self.to_database_value(attribute_name, value)
        return self

    def attribute_is(self, attribute_name, value):
        '''name=is.value'''
        self.query_params[attribute_name] = 'is.%s' % self.to_database_value(attribute_name, value)
        return self

    def attributes_eq(self, attributes_dict):
        '''name=eq.value'''
        for key, value in attributes_dict.items():
            self.query_params[key] = f'eq.{self.to_database_value(key, value)}'
        return self

    def attribute_in(self, attribute_name, values):
        '''or=(name.eq.value1,name.eq.value2)'''
        name_values = [
            f'{attribute_name}.eq.{self.to_database_value(attribute_name, value)}'
            for value in values]
        self.query_params['or'] = f'({",".join(name_values)})'
        return self

    def attribute_like(self, attribute_name, value, case_sensitive):
        '''
        LIKE operator (use * in place of %) or ILIKE (if case_sensitive is False)
        name=like.value or name=ilike.value
        
        We could also use full-text search, see:
        http://postgrest.org/en/v6.0/api.html#fts
        https://www.compose.com/articles/mastering-postgresql-tools-full-text-search-and-phrase-search/
        '''
        self.query_params[attribute_name] = f'{"like" if case_sensitive else "ilike"}. \
                                               {self.to_database_value(attribute_name, value)}'
        return self

    def order_asc(self, attribute_name):
        '''order=name1.asc,name2.asc'''
        try:
            self.query_params['order'] += ','
        except KeyError:
            self.query_params['order'] = ''
        self.query_params['order'] += f'{attribute_name}.asc'
        return self

    def order_desc(self, attribute_name):
        '''order=name1.desc,name2.desc'''
        try:
            self.query_params['order'] += ','
        except KeyError:
            self.query_params['order'] = ''
        self.query_params['order'] += f'{attribute_name}.desc'
        return self

    def limit(self, count):
        '''limit=count'''
        self.query_params['limit'] = str(count)

    def max(self, attribute_name):
        '''Order by attribute descending, keep only the first one'''
        self.order_desc(attribute_name).limit(1)
        return self
