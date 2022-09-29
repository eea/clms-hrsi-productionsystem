'''
_summary_

:raises CsiInternalError: _description_
:raises exception: _description_
:raises exception: _description_
:raises NotImplementedError: _description_
'''
from datetime import datetime
from time import sleep
import time

from .job_status import JobStatus
from .other_job import OtherJob
from ....util.exceptions import CsiInternalError
from ....util.log_util import temp_logger


class LoopedJob(OtherJob):
    '''
    An 'other job' that executes in a loop.

    :param OtherJob: _description_
    :type OtherJob: _type_
    :raises CsiInternalError: _description_
    :raises exception: _description_
    :raises exception: _description_
    :raises exception: _description_
    :raises Exception: _description_
    '''

    @staticmethod
    def static_start(job_name,
                     job_sub_type,
                     next_log_level,
                     loop_sleep,
                     repeat_on_fail=False):
        '''
        Create a new instance and start it.

        :param job_name: _description_
        :type job_name: _type_
        :param job_sub_type: _description_
        :type job_sub_type: _type_
        :param next_log_level: _description_
        :type next_log_level: _type_
        :param loop_sleep: _description_
        :type loop_sleep: _type_
        :param repeat_on_fail: _description_, defaults to False
        :type repeat_on_fail: bool, optional
        :raises CsiInternalError: _description_
        :raises exception: _description_
        '''

        # Get or create a looped job
        try:
            job = OtherJob.get_or_post(job_name, job_sub_type, repeat_on_fail=repeat_on_fail)
        except Exception as exception:
            temp_logger.error('An error occurred while attempting to get or post the following '
                              'service job : %s! \nError : %s', job_name, exception)

            # Pause service if it raises a critical error
            while True:
                time.sleep(1)

        # Set log level
        job.next_log_level = next_log_level

        # When the previous status is "started", for now we can't post a status
        # change to "started". In fact ideally we should check the current
        # status and switch to "started" only if it not "started" already. In
        # the meantime we ignore all errors with a try/except which is not
        # ideal, but is acceptable as there is a chance that we won't miss any
        # major errors.
        try:
            job.post_new_status_change(JobStatus.started)
        except Exception:
            pass

        # Start execution.
        # Call: ChildJob.start -> self.wrapped_start -> [ChildJob subtype].looped_start
        try:
            super(LoopedJob, job).start(loop_sleep)

        # If an error is raised -> change the service job status
        except CsiInternalError as exception:

            # Update the status
            job.post_new_status_change(
                JobStatus.internal_error,
                error_subtype=exception.subtype,
                error_message=exception.message
            )
            temp_logger.error("A CsiInternalError occurred during the service '%s' execution ! "\
                              "\nCsiInternalError : %s", job_name, exception)
            # Pause service if it raises a critical error, except for the
            #  "Monitoring" service which should remain up.
            if job_name != 'monitor':
                while True:
                    time.sleep(1)

    def wrapped_start(self, *args):
        '''
        Start job execution, wrapped by ChildJob.start
        Start the child execution in an infinite loop.

        :raises exception: _description_
        :raises exception: _description_
        '''

        loop_sleep = args[0]
        #initalize the eror count for the publication
        compteur_erreur = 0

        # Infinite loop
        while True:

            # Start time of the current execution
            start_time = datetime.utcnow()

            try:
                # Start child execution
                self.looped_start(*args)
                #remet le compteur d'errreur à 0
                compteur_erreur = 0 #normalement le code s'éxecute

            # If exception is critical stop the system,
            # else print it and continue
            except CsiInternalError as exception:
                if (self.parent_job.name == "job-publication" and
                exception.subtype == "Job Status Transition Error"):

                    #Si le compteur d'erreur relève trois erreur
                    # consécutives alors, nous relevons
                    # l'erreur, et partons dans la boucle infinie
                    if compteur_erreur >= 3:
                        raise exception

                    #ajoute au compteur une erreur suplémentaire
                    compteur_erreur+=1
                    self.logger.warning(f' {format(exception.subtype)}'
                                        f'Error occurence: {format(compteur_erreur)}/3')
                else:
                    raise exception

            # Print exception and continue
            except Exception:
                self.logger.exception(f'{self.parent_job.name} error')

            # How much time did the execution take, in seconds
            execution_time = (datetime.utcnow() - start_time).seconds

            # Remove this time from the time between two executions.
            # Min value = 0
            sleep_aux = max(0, loop_sleep - execution_time)

            # Sleep until next request
            sleep(sleep_aux)

    def looped_start(self, *args):
        '''
        Start job execution in an infinite loop.
        Must be implemented by the child classes.

        :raises NotImplementedError: Unimplemented function
        '''
        raise NotImplementedError('Unimplemented function')
    