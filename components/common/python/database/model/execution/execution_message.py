from ...rest.rest_database import RestDatabase


class ExecutionMessage(RestDatabase):
    '''
    Message attached to a job execution.
    Many to many relationship between many job executions and messages:
    Each job can have many messages, and each unique message can be attached
    to many executions (identical messages are not duplicated).

    :param body: Message body
    '''

    # Database table name
    TABLE_NAME = "execution_messages"

    def __init__(self, body=''):

        self.body = body

        # Call the parent constructor AFTER all the attributes are initialized
        super().__init__(ExecutionMessage.TABLE_NAME)

    #
    # Build a string for PostgREST query parameters

    def body_like(self, value, case_sensitive=False):
        return super().attribute_like('body', value, case_sensitive)
