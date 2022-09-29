'''
This module is responsible of the workload distribution logic implementation.
It interacts through nomad and openstack APIs to monitor and trigger actions
on workerssucha as updating their health status, create and destroy them...
Its actions are based on health status, duration of activity/inactivity,
workload diversity and >computing ressources disponibility.
'''
import logging
import datetime
import os
import math

import nomad
import openstack
import keystoneauth1

from ...common.python.database.model.job.looped_job import LoopedJob
from ...common.python.database.model.job.system_parameters import SystemPrameters
from ...common.python.database.model.job.job_types import JobTypes
from ...common.python.database.logger import Logger
from ...common.python.util.datetime_util import DatetimeUtil
from ...common.python.util.exceptions import CsiInternalError, CsiExternalError
from .csi_openstack import ask_openstack_to_create_worker, delete_worker_with_openstack, \
                           get_openstack_flavor_vcpus
from .csi_nomad import get_pending_nomad_jobs, get_ready_and_unallocated_nomad_nodes, \
                       node_hasnt_run_an_alloc_recently

# check if environment variable is set, exit in error if it's not
from ...common.python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")



def raise_internal_error_if_env_not_set_or_empty(env_variable_name: str):
    '''
    Checks if the a specified environment variable is set-up.

    :param env_name: name of the environment variable the set-up is to be checked
    :type env_name: str
    :raises CsiInternalError: CSI error to be raised if internal error is detected
    '''
    # Check for existence
    if env_variable_name not in os.environ:
        raise CsiInternalError('Missing env var',
                                f'{env_variable_name} environment variable not found')
    # Check for initilization
    if os.environ[env_variable_name] == '':
        raise CsiInternalError('Empty env var',
                                f'{env_variable_name} is set but contains an empty string')


def get_openstack_worker_age(openstack_worker)->datetime.timedelta:
    '''
    Get the duration time since when the specified openstack worker has been
    updated last.

    :param openstack_worker: The openstack worker which age is to be monitored
    :type openstack_worker: openstack.connnection.Connection
    :return: The duration time since when the specified openstack worker has been
    updated last.
    :rtype: datetime.timedelta
    '''
    updated_date = DatetimeUtil.fromRfc3339(openstack_worker.updated)
    age = datetime.datetime.utcnow() - updated_date.replace(tzinfo=None)
    return age


def round_value(value:float, round_high=False)->tuple:
    '''
    Rounds a value, returning the rounded value and the decimal part of the fraction.

    :param value: value to be rounded.
    :param round_high: parameter notifying that 0.5 decimal should be rounded to
        upper value.
    :return: The rounded up value.
    :rtype: tuple(int, float)
    '''
    decimal_part = value % 1
    if round_high and decimal_part == 0.5:
        return int(value//1 + 1), 0
    if decimal_part > 0.5:
        decimal_part = 0
    return round(value), decimal_part


def scale_up_workers(worker_need_dictionary:dict,
                     total_n_workers:int,
                     n_min_worker:int
                     )->dict:
    '''
    Scales up the number of workers to deploy if its below the minimum.

    :param worker_need_dictionary: dictionary in which are stored the number
        of workers required per job type.
    :type worker_need_dictionnary: dict
    :param total_n_workers: total number of worker required by all the job types.
    :type total_n_workers: int
    :param n_min_worker: minimum number of workers that can be deployed.
    :type n_min_worker: int
    :return: The updated worker need dictionnary
    :rtype: dict
    '''
    # Compute the factor to increase the number of workers asked by each job type
    factor = math.ceil(n_min_worker/total_n_workers)

    for key in worker_need_dictionary.keys():
        worker_need_dictionary[key] = worker_need_dictionary[key] * factor
    return worker_need_dictionary


def scale_down_workers(worker_need_dictionary:dict,
                       total_n_workers:int,
                       n_max_worker:int
                       )->dict:
    '''
    Scales down the number of workers to deploy if its above the maximum.

    :param worker_need_dictionary: dictionary in which are stored the number
        of workers required per job type.
    :type worker_need_dictionnary: dict
    :param total_n_workers: total number of worker required by all the job types.
    :type param_total_n_workers: int
    :param n_max_worker: maximum number of workers that can be deployed.
    :type n_max_worker: int
    :return: The updated worker need dictionnary
    :rtype: dict
    '''
    result_dict = {}
    decimal_dict = {}

    # Compute the factor to reduce the number of workers asked by each job type
    factor = math.ceil(total_n_workers/n_max_worker)

    for key in worker_need_dictionary.keys():
        result_dict[key], decimal_dict[key] = round_value(
            worker_need_dictionary[key] / factor, round_high=True)

    # If the total number of workers to deploy is still above the maximum,
    # we perform a stricter down scale.
    if sum(result_dict.values()) > n_max_worker:
        result_dict = {}
        decimal_dict = {}

        for key in worker_need_dictionary.keys():
            result_dict[key], decimal_dict[key] = round_value(
                worker_need_dictionary[key] / factor)

    # If the number of workers to deploy is below the maximum, we re-add workers to the
    # job-types that need it the most (highest decimal), until the maximum is reached.
    if sum(result_dict.values()) < n_max_worker:
        diff = n_max_worker - sum(result_dict.values())

        # Find the keys with the highest decimals
        for key in sorted(decimal_dict, key=decimal_dict.get, reverse=True)[:diff]:
            result_dict[key] += 1

    return result_dict


def get_number_of_workers_to_create(logger: Logger,
                                    worker_need_per_job_type: dict,
                                    max_number_of_workers: int,
                                    current_number_of_workers: int
                                    )->dict:
    '''
    Computes the needs in os-worker creation by type according to the already
    existing ones, the jobs to be created and the type of those.add()

    :param logger: logger to be used to log
    :type logger: Logger
    :param worker_need_per_job_type: dictionnary containing the amount of worker needed per job type
    :type worker_need_per_job_type: dict
    :param max_number_of_workers: maximum number of workers that can be deployed.
    :type max_number_of_workers: int
    :param current_number_of_workers: dictionnary containing the amount of worker currently
                                      existing per flavor
    :type current_number_of_workers: dict
    :return: dictionnary containing the amount of os-worker to create for each type of flavor
    :rtype: dict
    '''
    # Compute the total number of workers needed by all the job types
    total_n_workers_to_create = sum(worker_need_per_job_type.values())

    # Compute the total number of possible workers avaiable for allocation
    n_available_instances_for_creation = max_number_of_workers - current_number_of_workers

    if n_available_instances_for_creation <= 0:
        logger.info('There is no available worker instance for creation')
    elif total_n_workers_to_create <= 0:
        logger.info('There is no need to create new worker instances')
    else:
        logger.debug(f'we need at most {total_n_workers_to_create}: '
                     f'new worker instances to execute '
        )
        logger.debug(f'the max allowed number of worker instances is: '
                     f'{max_number_of_workers}'
        )

        n_min_worker_creation_by_batch = 3
        if total_n_workers_to_create < n_min_worker_creation_by_batch:
            logger.debug(
                f'we create at least {n_min_worker_creation_by_batch} in '
                f'case new jobs are queued during worker creation'
                )
            worker_need_per_job_type = scale_up_workers(worker_need_per_job_type,
                total_n_workers_to_create, n_min_worker_creation_by_batch)

        # Update the the sum value, as it might have been updated on previous step
        total_n_workers_to_create = sum(worker_need_per_job_type.values())


        n_max_worker_creation_by_batch = min(10, n_available_instances_for_creation)
        if total_n_workers_to_create > n_max_worker_creation_by_batch:
            logger.debug(
                f'but don\'t create more than {n_max_worker_creation_by_batch} '
                f'by batch'
                )
            worker_need_per_job_type = scale_down_workers(worker_need_per_job_type,
                total_n_workers_to_create, n_max_worker_creation_by_batch)

        # Update the the sum value, as it might have been updated on previous step
        total_n_workers_to_create = sum(worker_need_per_job_type.values())

        logger.info(
            f'we ask for the creation of {total_n_workers_to_create} new worker '
            f'instances'
            )
        return worker_need_per_job_type

    # If we reach this point it means we should not create any new worker, so we
    # set all the values in the dictionary to 0.
    for job_type in worker_need_per_job_type.keys():
        worker_need_per_job_type[job_type] = 0

    return worker_need_per_job_type


def get_names_of_workers_to_create(logger: Logger,
                                   current_workers_names,
                                   max_number_of_workers,
                                   n_workers_to_create
                                   )->list:
    '''
    _summary_

    :param logger: _description_
    :type logger: Logger
    :param current_workers_names: _description_
    :type current_workers_names: _type_
    :param max_number_of_workers: _description_
    :type max_number_of_workers: _type_
    :param n_workers_to_create: _description_
    :type n_workers_to_create: _type_
    :return: _description_
    :rtype: _type_
    '''
    complete_list_of_possible_names = [
        f'os-worker-{str(number).zfill(3)}'
        for number in list(range(1, max_number_of_workers + 1))
    ]

    names_of_available_workers = sorted(list(
        set(complete_list_of_possible_names) - set(current_workers_names)
        ))
    names_of_workers_to_create = names_of_available_workers[:n_workers_to_create]
    logger.debug(f'workers to create: {names_of_workers_to_create}')

    return names_of_workers_to_create


def set_worker_health(worker:dict,
                      logger:Logger
                      )->None:
    '''
    Method applying health policy on nomad instances according to their age.
    The age of a nomad agent is to be undcerstood as the duration since when
    an action has been taken on it.

    :param worker: openstack worker instance
    :type worker: dict
    :param logger: logger instance to be used
    :type logger: Logger
    '''
    # If a worker has no Nomad agent allocated, it can only be in intermediate
    # or stale state.
    if worker['nomad'] is None:
        # Worker age computation.
        worker_age = get_openstack_worker_age(worker['openstack'])
        logger.debug(f'     - worker {worker["name"]} has not had an allocated nomad agent for {worker_age}')
        # Warning: don't set this age to a too low value, because if a worker is
        # being created and is not yet ready it can be seen as stale and will be
        # destroyed by error.
        # For information, a worker creation usually takes several minutes.
        # So 15 minutes seems a reasonnable value.
        age_threshold = datetime.timedelta(minutes=SystemPrameters().get(logger.debug).max_time_for_worker_without_nomad_allocation)
        if worker_age > age_threshold:
            logger.debug(f'         - worker {worker["name"]} has not had an allocated nomad agent for more than {age_threshold}')
            worker['health'] = 'stale'
        else:
            logger.debug(f'         - worker {worker["name"]} can still wait to be allocated for {age_threshold - worker_age}')
            worker['health'] = 'intermediate'
        logger.debug(f'         - worker {worker["name"]} health set at -- {worker["health"]}')
    else:
        if worker['ip'] == 'not available':
            logger.debug(f'         - worker { worker["name"]} has an allocated agent but no IP. He is still accessible.')
            worker['health'] = 'intermediate'
            logger.debug(f'         - worker {worker["name"]} health set at -- {worker["health"]}')
        else:
            # Worker is seen from OpenStack and Nomad, and its IP is set:
            # everything is OK.
            worker['health'] = 'healthy'



def filter_workers_by_health(workers: dict,
                             health: str
                             )->dict:
    '''
    _summary_

    :param workers: _description_
    :type workers: dict
    :param health: _description_
    :type health: str
    :return: _description_
    :rtype: _type_
    '''
    return [
        worker
        for _, worker in workers.items()
        if worker['health'] == health
    ]


def filter_worker_list_by_flavor(worker_list: list,
                                 flavor_name: str
                                 )->dict:
    '''
    _summary_

    :param worker_list: _description_
    :type worker_list: list
    :param flavor_name: _description_
    :type flavor_name: str
    :return: _description_
    :rtype: _type_
    '''
    return [
        worker
        for worker in worker_list
        if worker['openstack']['flavor']['original_name'] == flavor_name
    ]


def filter_nomad_nodes_by_flavor(nomad_nodes: list,
                                 workers: dict,
                                 flavor_name: str
                                 )->dict:
    '''
    _summary_

    :param nomad_nodes: _description_
    :type nomad_nodes: list
    :param workers: _description_
    :type workers: dict
    :param flavor_name: _description_
    :type flavor_name: str
    :return: _description_
    :rtype: _type_
    '''
    filtered_nomad_nodes = []
    for nomad_node in nomad_nodes:
        if (
            nomad_node['Address'] in workers.keys()
            and workers[nomad_node['Address']][
                'openstack']['flavor']['original_name'] == flavor_name
        ):
            filtered_nomad_nodes.append(nomad_node)

    return filtered_nomad_nodes


def get_openstack_servers(logger: Logger,
                          openstack_client: openstack.connect)->list:
    '''
    Run 'openstack server list' command through the Python API and returns the result
    formated intoa dictionnary

    :param logger: th elogger
    :type logger: Logger
    :raises CsiExternalError: OpenStack connection error - Internal server error
    :raises CsiExternalError: OpenStack connection error - Connection failure
    :raises CsiExternalError: OpenStack connection error - HTTP error
    :raises CsiExternalError: OpenStack connection error - Unknown connection error
    :raises CsiExternalError: OpenStack connection error - External error
    :return: dictionnary containing the result of the command
    :rtype: dict
    '''
    servers = []
    try:
        servers = openstack_client.list_servers()
    except keystoneauth1.exceptions.http.InternalServerError as error:
        logger.error(f'OpenStack connection error while trying to get the list of '
                     f'servers. keystoneauth1.exceptions.http.InternalServerError: '
                     f'{error}')

        raise CsiExternalError('OpenStack connection error',
                               'Error while trying to get the list of servers : '\
                               'keystoneauth1.exceptions.http.InternalServerError') \
                                   from keystoneauth1.exceptions.http.InternalServerError

    except keystoneauth1.exceptions.connection.ConnectFailure as error:
        logger.error(f'OpenStack connection error while trying to get the list of '
                     f'servers. keystoneauth1.exceptions.connection.ConnectFailure: '
                     f'{error}')
        raise CsiExternalError('OpenStack connection error',
                               'Error while trying to get the list of servers : ' \
                               'keystoneauth1.exceptions.connection.ConnectFailure') \
                                    from error

    except openstack.exceptions.HttpException as error:
        logger.error(f'HTTP error {error.response.status_code} while '
                     f'trying to get the list of servers with OpenStack: {error}')
        raise CsiExternalError('OpenStack connection error',
                               'Error while trying to get the list of servers : '\
                                f'HTTP error {error.response.status_code}') \
                                    from error

    except keystoneauth1.exceptions.connection.UnknownConnectionError as error:
        logger.error(f'OpenStack connection error while trying to get the list of '
                     f'servers. keystoneauth1.exceptions.connection.UnknownConnectionError: '
                     f'{error}')
        raise CsiExternalError('OpenStack connection error',
                               'Error while trying to get the list of servers : ' \
                               'keystoneauth1.exceptions.connection.UnknownConnectionError') \
                                   from keystoneauth1.exceptions.connection.UnknownConnectionError

    except Exception as error:
        logger.error(f'Unknown OpenStack connection error while trying to get the list of '
                     f'servers : {error}')
        raise CsiExternalError('OpenStack connection error',
                               'Error while trying to get the list of servers : Unknown error') \
                                   from error

    return servers


def get_non_worker_active_services_state(logger: Logger,
                                         nomad_client: nomad.Nomad,
                                         openstack_client: openstack.connect)->dict:
    '''
    Extract the non-worker active services state and store them in a dictionnary to return

    :param logger: the logger instance to be used
    :type logger: Logger
    :return: dictionnary containing the services id associated to their states
    :rtype: dict
    '''
    logger.info('   getting OpenStack services...')
    # Initialization of the list by selecting the servers not starting with os-worker-
    # and that are ACTIVE
    openstack_services = [
        server
        for server in get_openstack_servers(logger, openstack_client)
        if not server.name.startswith('os-worker-')
        if bool(server.status == "ACTIVE")
    ]
    # Getting their number of vCPUs by interogating their flavor & setting-up their ip adress & key
    network_name = 'private_magellium'
    services = {}
    for openstack_service in openstack_services:
        if network_name in openstack_service.addresses:
            ip_address = openstack_service.addresses[network_name][0]['addr']
            key = ip_address
        else:
            # If we can't get the IP address it certainly means that the
            # instance is either being created or deleted. So we use the ID as a
            # key for the service dictionnary.
            ip_address = 'not available'
            key = openstack_service.id
        flavor_name = openstack_service['flavor']['original_name']
        nb_vcpus = get_openstack_flavor_vcpus(openstack_client, flavor_name)
        services[key] = {
            'ip': ip_address,
            'name': openstack_service['name'],
            'openstack': openstack_service,
            'nomad': None,
            'health': 'unknown',
            'flavor_name': flavor_name,
            'nb_vcpus': nb_vcpus
        }
    # Extracting the complementary data from the corresponding nomad workers
    logger.info('   getting Nomad services...')
    nodes = nomad_client.nodes.get_nodes()

    nomad_services = [
        node
        for node in nodes
        if (
            not node['Name'].startswith('os-worker')
            and
            node['Status'] == 'ready'
            )
    ]

    for nomad_service in nomad_services:
        ip_address = nomad_service['Address']
        if ip_address in services:
            services[ip_address]['nomad'] = nomad_service
        else:
            name = nomad_service['Name']
            logger.warning(
                f'service {name} ({ip_address}) is not referenced by OpenStack which can '
                f'happens when OpenStack has just deleted the worker and the '
                f'Nomad server is not yet aware of that')

    return services


def get_workers_state(logger: Logger,
                      nomad_client: nomad.Nomad,
                      openstack_client: openstack.connect)->dict:
    '''
    Extracts the os-worker state and stores them in a dictionnary to return

    :param logger: The logger instance
    :type logger: Logger
    :return: A dictionnary containing the worker id associated to their states
    :rtype: dict
    '''
    logger.info('   getting OpenStack workers...')
    # Initialization of the list by selecting the servers starting with os-worker-
    openstack_workers = [
        server
        for server in get_openstack_servers(logger, openstack_client)
        if server.name.startswith('os-worker-')
    ]
    # Getting their number of vCPUs by interogating their flavor & setting-up their ip adress & key
    network_name = 'private_magellium'
    workers = {}
    for openstack_worker in openstack_workers:
        if network_name in openstack_worker.addresses:
            ip_address = openstack_worker.addresses[network_name][0]['addr']
            key = ip_address
        else:
            # If we can't get the IP address it certainly means that the
            # instance either being created or deleted. So we use the ID as a
            # key for the workers dictionnary.
            ip_address = 'not available'
            key = openstack_worker.id
        flavor_name = openstack_worker['flavor']['original_name']
        nb_vcpus = get_openstack_flavor_vcpus(openstack_client, flavor_name)
        workers[key] = {
            'ip': ip_address,
            'name': openstack_worker['name'],
            'openstack': openstack_worker,
            'nomad': None,
            'health': 'unknown',
            'flavor_name': flavor_name,
            'nb_vcpus': nb_vcpus
        }

    # Extracting the complementary data from the corresponding nomad workers
    logger.info('   getting Nomad workers...')
    nodes = nomad_client.nodes.get_nodes()

    nomad_workers = [
        node
        for node in nodes
        if (
            node['Name'].startswith('os-worker')
            and
            node['Status'] == 'ready'
            )
    ]

    for nomad_worker in nomad_workers:
        ip_address = nomad_worker['Address']
        if ip_address in workers:
            workers[ip_address]['nomad'] = nomad_worker
        else:
            name = nomad_worker['Name']
            logger.warning(
                f'worker {name} ({ip_address}) is not referenced by OpenStack which can '
                f'happens when OpenStack has just deleted the worker and the '
                f'Nomad server is not yet aware of that')

    return workers


def switch_sleeping_workers_to_ineligible(logger: Logger,
                                          healthy_workers:dict,
                                          nomad_client: nomad.Nomad
                                          )->None:
    '''
    Sets the health status of sleeping os-workers to ineligible.

    :param logger: logger instance to log
    :type logger: Logger
    :param healthy_workers: dictionnary containing dictionnaries representing healthy os-workers
    :type healthy_workers: dict
    '''
    # Before deleting the inactive workers we first need to be sure
    # there will be no new allocations by the time the instance is
    # actually destroyed. So we set the corresponding Nomad nodes to
    # ineligible first.

    sleeping_workers = [
        worker
        for worker in healthy_workers
        if node_hasnt_run_an_alloc_recently(nomad_client, worker['nomad'])
    ]

    n_sleeping_workers = len(sleeping_workers)
    if n_sleeping_workers == 0:
        logger.debug('there is no sleeping workers nodes to switch to ineligible')
    else:
        logger.info(f'there are {n_sleeping_workers} sleeping workers, set some '
                    f'of them as ineligible so they can be destroyed')

        if n_sleeping_workers != 1:
            # Only switch a small part of the nodes. The reason is that after the
            # switch, the workers will be deleted which takes some time, and some
            # new jobs might been submit in the meantime and will need some workers.
            n_nomad_nodes_to_switch = n_sleeping_workers // 3
            # operator "//" is the integer division, which might lead to 0. Just
            # be sure to switch at least one node.
            if n_nomad_nodes_to_switch == 0:
                n_nomad_nodes_to_switch = 1

            logger.info(f'only switch eligibility on {n_nomad_nodes_to_switch} of them')
        else:
            n_nomad_nodes_to_switch = n_sleeping_workers

        n_max_eligibility_switch = 10

        if n_nomad_nodes_to_switch > n_max_eligibility_switch:
            logger.debug(f'but don\'t switch eligibility on more than '
                         f'{n_max_eligibility_switch} by batch')

            n_nomad_nodes_to_switch = n_max_eligibility_switch

        nomad_nodes_to_switch = [
            worker['nomad']
            for worker in sleeping_workers[:n_nomad_nodes_to_switch]
        ]
        for node in nomad_nodes_to_switch:
            logger.debug(f'set inegibility for Nomad node {node["ID"]}')
            nomad_client.node.eligible_node(node['ID'], ineligible=True)


def remove_ineligible_sleeping_workers(logger: Logger,
                                       worker_list,
                                       nomad_client: nomad.Nomad,
                                       openstack_client: openstack.connect
                                       )->None:
    '''
    Destroys ineligible os-workers from the worker pool

    :param logger: logger instance to be used
    :type logger: Logger
    :param worker_list: dictionnary containing the workers
    :type worker_list: dict
    '''

    workers_to_delete = [
        worker
        for worker in worker_list
        if (
            worker['nomad']['SchedulingEligibility'] == 'ineligible'
            and node_hasnt_run_an_alloc_recently(nomad_client, worker['nomad'])
        )
    ]

    n_workers_to_delete = len(workers_to_delete)
    if n_workers_to_delete == 0:
        logger.debug('there is no sleeping workers to destroy')
    else:
        logger.info(f'there are {n_workers_to_delete} workers to destroy')
        n_max_worker_deletion_by_batch = 10

        if n_workers_to_delete > n_max_worker_deletion_by_batch:
            logger.debug(f'but don\'t destroy more than {n_max_worker_deletion_by_batch} \
                         wokers by batch')
            n_workers_to_delete = n_max_worker_deletion_by_batch

        workers_to_delete = workers_to_delete[:n_workers_to_delete]

        logger.info('asking OpenStack to delete the workers...')
        for worker in workers_to_delete:
            delete_worker_with_openstack(logger, openstack_client, worker)

def check_worker_pool_management_environment()->None:
    '''
    Check for correct setting up of the following environment variables:
        * OS_USERNAME
        * OS_PASSWORD
        * CSI_NOMAD_SERVER_IP
        * CSI_HTTP_API_INSTANCE_IP

    Raises an error otherwise.
    '''
    raise_internal_error_if_env_not_set_or_empty('OS_USERNAME')
    raise_internal_error_if_env_not_set_or_empty('OS_PASSWORD')

    raise_internal_error_if_env_not_set_or_empty('CSI_NOMAD_SERVER_IP')
    raise_internal_error_if_env_not_set_or_empty('CSI_HTTP_API_INSTANCE_IP')


def monitor_current_openstack_and_nomad_state(logger:Logger,
                                              nomad_client: nomad.Nomad,
                                              openstack_client: openstack.connect
                                              )->tuple:
    '''
    _summary_

    :raises Exception: _description_
    '''

    # Extracting workers state list
    logger.info('Extracting workers state from OpenStack and Nomad...')
    try:
        workers = get_workers_state(logger, nomad_client, openstack_client)
    except CsiExternalError as error:
        logger.warning('Communication with OpenStack couldn\'t be established so we skip workers management.')
        logger.warning(f'Error raised : {error.message}')
        return

    # Updating workers health status
    logger.info('\nComputing health for os-workers...')
    for _, worker in workers.items():
        set_worker_health(worker, logger)

    # Sorting workers by health
    healthy_workers = filter_workers_by_health(workers, 'healthy')
    workers_in_intermediate_state = filter_workers_by_health(workers, 'intermediate')
    stale_workers = filter_workers_by_health(workers, 'stale')
    # Displaying sorting results
    logger.info(f'  number of healthy workers................ {len(healthy_workers)}')
    logger.info(f'  number of workers in intermediate state.. {len(workers_in_intermediate_state)}')
    logger.info(f'  number of stale workers.................. {len(stale_workers)}\n')


    # Extracting services state list
    logger.info('Extracting services state from OpenStack and Nomad...')
    try:
        services = get_non_worker_active_services_state(logger, nomad_client, openstack_client)
    except CsiExternalError as error:
        logger.warning('Communication with OpenStack couldn\'t be established so we skip worker management.')
        logger.warning(f'Error raised : {error.message}')
        return

    logger.info('')
    logger.info('Computing vCPUs ressources currently used...')
    logger.debug('  Computing vCPUs ressources currently used by services...')
    current_number_of_vcpu_used_by_services = 0
    for service in services.values():
        current_number_of_vcpu_used_by_services += service['nb_vcpus']
    logger.debug(f'      - total number of currently running services........{len(services)}')
    logger.debug(f'      - total number of vCPUs currently used by services..{current_number_of_vcpu_used_by_services}')

    # Extracting the type of VMs that are used by the workers
    logger.debug('  Computing vCPUs ressources currently used by workers...')
    #TODO GENERALIZATION oF THE FOLLOWING BLOCK
    current_number_of_vcpus_used_by_workers = 0
    current_number_of_workers = len(workers)
    current_number_of_eo2medium = 0
    current_number_of_hmmedium = 0
    current_number_of_hmlarge = 0
    current_number_of_hmxlarge = 0

    for worker in workers.values():
        current_number_of_vcpus_used_by_workers += worker['nb_vcpus']
        current_number_of_eo2medium += int("eo2.medium" in worker['flavor_name'])
        current_number_of_hmmedium += int("hm.medium" in worker['flavor_name'])
        current_number_of_hmlarge += int("hm.large" in worker['flavor_name'])
        current_number_of_hmxlarge += int("hm.xlarge" in worker['flavor_name'])

    # Displaying this collected data
    logger.debug(f'      - total number of currently used workers...........{current_number_of_workers}')
    logger.debug(f'      - total number of vCPUs currently used by workers..{current_number_of_vcpus_used_by_workers}')
    logger.debug('      Repartition by VM\' flavor')
    logger.debug(f'         - number of eo2.medium..{current_number_of_eo2medium}')
    logger.debug(f'         - number of hm.medium...{current_number_of_hmmedium}')
    logger.debug(f'         - number of hm.large....{current_number_of_hmlarge}')
    logger.debug(f'         - number of hm.xlarge...{current_number_of_hmxlarge}')

    current_number_of_vcpus_used = current_number_of_vcpu_used_by_services + \
                                    current_number_of_vcpus_used_by_workers
    logger.info(f'  Total number of vCPUs currently used...{current_number_of_vcpus_used}\n')

    return current_number_of_vcpus_used, \
           stale_workers, \
           workers_in_intermediate_state, \
           workers, \
           current_number_of_workers, \
           healthy_workers

class WorkerPoolManagement(LoopedJob):
    '''
    This service manage the pool of workers as VM instance as viewed by
    OpenStack and the Nomad clients that run on workers. The management observe
    needs of workers based of queued processing jobs in Nomad.
    '''
    
    # Getting the handle on the openstack client
    openstack_client = openstack.connect()
    # Getting the handle on the nomad client
    nomad_client = nomad.Nomad()
        

    # Getting the handle on the openstack client
    openstack_client = openstack.connect()
    # Getting the handle on the nomad client
    nomad_client = nomad.Nomad()


    @staticmethod
    def start()->None:
        '''
        Start execution in an infinite loop.
        '''

        LoopedJob.static_start(
            job_name='manage-worker-pool',
            job_sub_type=WorkerPoolManagement,
            next_log_level=logging.getLevelName('DEBUG'),
            # Time in seconds between two requests to the database for new jobs.
            loop_sleep=5,
            repeat_on_fail=True)


    def looped_start(self, *args)->None:
        '''
        Start the worker pool management, wrapped by OtherJob.wrapped_start
        '''

        # -------------------------------------------------------
        # Setting up the environment
        if not self.logger:
            raise Exception('Logger must be initialized.')
        logger = self.logger

        logger.info('')
        logger.info('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        logger.info('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        logger.info('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        logger.info('start worker pool management new loop')

        check_worker_pool_management_environment()

        # Extracting key system parameters
        logger.info('Extracting system parameters...')
        max_number_of_workers = SystemPrameters().get(logger.debug).max_number_of_worker_instances
        logger.info(f'      - max number of worker is {max_number_of_workers}')
        max_number_of_vcpus = SystemPrameters().get(logger.debug).max_number_of_vcpus
        logger.info(f'      - max number of vCPUs is {max_number_of_vcpus}')
        #TODO make it a system parameter
        max_accepted_vcpus_ratio = 0.95
        logger.info(f'      - max ratio of allowded vCPUs to be used is: {round(100.0*max_accepted_vcpus_ratio,1)}%\n')

        # -------------------------------------------------------
        # Making sure the system is in an optimal shape and extracting key data
        current_number_of_vcpus_used, \
        stale_workers, \
        workers_in_intermediate_state , \
        workers, \
        current_number_of_workers, \
        healthy_workers = monitor_current_openstack_and_nomad_state(logger, self.nomad_client, self.openstack_client)

        # If for a reason or another we reach the point of saturation in terms of vcpus,
        # we try to free some computationnal power by destroying the os-workers in ERROR.
        logger.info('Examining the need to free ressources by forced desallocation of stale VMs...')
        ratio_of_allowded_vcpus_used = current_number_of_vcpus_used / max_number_of_vcpus
        logger.info(f'  current ratio of allowded vCPUs used...{round(100.0*ratio_of_allowded_vcpus_used,1)}%')
        if ratio_of_allowded_vcpus_used > max_accepted_vcpus_ratio:
            logger.info(f'      - ratio overcomes the max accepted value of {round(100.0*max_accepted_vcpus_ratio,1)}%')
            logger.info(f'      - deleting {len(stale_workers)} stale workers at once.')
            # delete all the stale workers
            for stale_worker in stale_workers:
                delete_worker_with_openstack(logger, self.openstack_client, stale_worker)

            # Updating key data
            current_number_of_vcpus_used, \
            stale_workers, \
            workers_in_intermediate_state,\
            workers, \
            current_number_of_workers, \
            healthy_workers = monitor_current_openstack_and_nomad_state(logger, self.nomad_client, self.openstack_client)

        # -------------------------------------------------------
        # Core part of the worker pool management
        # First create some workers if there is some need to process new jobs.
        logger.info('')
        logger.info('Evaluating the need to create new workers and creating them on the fly...')

        job_type_list = JobTypes.get_job_type_list(logger)

        # Get the pending jobs list on the nomad client
        pending_nomad_jobs_dict = get_pending_nomad_jobs(self.nomad_client, job_type_list)

        # Dictionary storing the need of worker for each job type
        worker_need_per_job_type = {}

        # Iterate over the different pending Nomad jobs
        for job_type, pending_nomad_jobs_with_specified_flavor in pending_nomad_jobs_dict.items():
            logger.info(f'  Evaluating the needs in worker of flavor {job_type.WORKER_FLAVOR_NAME}...')
            # Filter intermediate workers based on their flavor (their configuration).
            workers_in_intermediate_state_with_proper_job_type = filter_worker_list_by_flavor(
                workers_in_intermediate_state, job_type.WORKER_FLAVOR_NAME)

            # Checking how many pending jobs there are for this job_type
            n_pending_jobs_with_proper_job_type = len(pending_nomad_jobs_with_specified_flavor)
            logger.info(f'      - number of PENDING Nomad jobs "{job_type.NOMAD_JOB_NAME}"'
                        f'that wait for a worker...{n_pending_jobs_with_proper_job_type}')

            # Selecting the inactive nomad nodes
            ready_and_unallocated_nomad_nodes = get_ready_and_unallocated_nomad_nodes(self.nomad_client)

            # Extracting the ones that match with the worker flavour
            ready_and_unallocated_nomad_nodes_with_proper_job_type = filter_nomad_nodes_by_flavor(
                ready_and_unallocated_nomad_nodes, workers, job_type.WORKER_FLAVOR_NAME)

            n_ready_and_unalocated_nomad_nodes_with_proper_job_type = len(ready_and_unallocated_nomad_nodes_with_proper_job_type)
            logger.info(f'      - number of READY Nomad jobs "{job_type.NOMAD_JOB_NAME}" that wait for a worker...{n_pending_jobs_with_proper_job_type}')

            # Computing the number of nomad nodes available for this sort of job
            n_workers_available_for_running = len(workers_in_intermediate_state_with_proper_job_type) +\
                                              n_ready_and_unalocated_nomad_nodes_with_proper_job_type
            logger.info(f'      - number of POTENTIAL Openstack workers that might run a PENDING job...{n_workers_available_for_running}')

            # Computing the delta offer/need
            n_needs_in_worker_of_specific_job_type = n_pending_jobs_with_proper_job_type - n_workers_available_for_running
            logger.info(f'  number of needed Nomad nodes of type {job_type.NOMAD_JOB_NAME}...{n_needs_in_worker_of_specific_job_type}')
            # Update the dictionary storing the need of worker for each job type
            worker_need_per_job_type[job_type] = n_needs_in_worker_of_specific_job_type

        # Computing the need in worker for each job type
        n_workers_to_create_per_job_type = get_number_of_workers_to_create(
            logger,
            worker_need_per_job_type,
            max_number_of_workers,
            current_number_of_workers
            )

        n_workers_to_create = sum(n_workers_to_create_per_job_type.values())

        # If we need and can create jobs, we do
        if n_workers_to_create > 0:
            current_workers_names = [
                worker['name']
                for worker in workers.values()
            ]
            names_of_workers_to_create = get_names_of_workers_to_create(
                logger, current_workers_names, max_number_of_workers, n_workers_to_create)

            n_workers_created = 0

            for job_type, n_workers in n_workers_to_create_per_job_type.items():
                logger.info(f'  Creating needed worker of flavor {job_type.WORKER_FLAVOR_NAME}...')
                for _ in range(n_workers):
                    worker_name = names_of_workers_to_create[n_workers_created]

                    logger.info(f'      - asking OpenStack to create worker {worker_name} of flavor {job_type.WORKER_FLAVOR_NAME}')
                    try:
                        ask_openstack_to_create_worker(logger, worker_name, \
                                                       job_type.WORKER_FLAVOR_NAME, self.openstack_client)
                    except openstack.exceptions.HttpException as error:
                        logger.error(
                            f'HTTP error {error.response.status_code} while '
                            f'creating worker {worker_name} of flavor '
                            f'{job_type.WORKER_FLAVOR_NAME} for Nomad job '
                            f'{job_type.NOMAD_JOB_NAME} with OpenStack: {error}'
                            )
                    except openstack.exceptions.ResourceTimeout as error:
                        logger.error(
                            f'OpenStack connection timeout error while '
                            f'creating worker {worker_name} of flavor '
                            f'{job_type.WORKER_FLAVOR_NAME} for Nomad job '
                            f'{job_type.NOMAD_JOB_NAME}: {error}'
                            )
                    except keystoneauth1.exceptions.connection.ConnectFailure as error:
                        logger.error(
                            f'OpenStack connection error while creating worker '
                            f'{worker_name} of flavor {job_type.WORKER_FLAVOR_NAME} '
                            f'for Nomad job {job_type.NOMAD_JOB_NAME}. '
                            f'keystoneauth1.exceptions.connection.ConnectFailure: '
                            f'{error}'
                            )
                    except keystoneauth1.exceptions.http.ServiceUnavailable as error:
                        logger.error(
                            f'OpenStack connection error while creating worker '
                            f'{worker_name} of flavor {job_type.WORKER_FLAVOR_NAME} '
                            f'for Nomad job {job_type.NOMAD_JOB_NAME}. '
                            f'keystoneauth1.exceptions.http.ServiceUnavailable: '
                            f'{error}'
                            )
                    except Exception as error:
                        logger.error(
                            f'Unknown OpenStack error while creating worker '
                            f'{worker_name} of flavor {job_type.WORKER_FLAVOR_NAME} '
                            f'for Nomad job {job_type.NOMAD_JOB_NAME}.'
                            f'{error}'
                            )

                    n_workers_created += 1


        # -------------------------------------------------------
        logger.info('')
        logger.info('Deleting stale workers if adequate...')
        for job_type, n_workers in n_workers_to_create_per_job_type.items():

            if n_workers != 0:
                logger.info(f'  There are some workers left to be created of flavor '
                            f'{job_type.WORKER_FLAVOR_NAME}, so we won\'t look for '
                            f'unused or stale workers to delete')
            else:
                logger.info(f'  Looking for unused workers of flavor '
                    f'{job_type.WORKER_FLAVOR_NAME} to delete...')

                # Filter healthy/stale workers based on their flavor (their configuration).
                filtered_healthy_workers = filter_worker_list_by_flavor(
                    healthy_workers, job_type.WORKER_FLAVOR_NAME)
                filtered_stale_workers = filter_worker_list_by_flavor(
                    stale_workers, job_type.WORKER_FLAVOR_NAME)

                switch_sleeping_workers_to_ineligible(logger, filtered_healthy_workers, self.nomad_client)
                remove_ineligible_sleeping_workers(logger, filtered_healthy_workers, self.nomad_client,self.openstack_client)

                # Finally clean up workers which are stale (i.e. on OpenStack instance
                # that is not seen by Nomad)
                #
                # This doesn't happen often, hopefully this number of workers is low,
                # and we choose to clean up one worker at a time to not stress the
                # system too much and to let maximum time to the previous tasks (and
                # most importantly the creation of new workers)
                if len(filtered_stale_workers) > 0:
                    logger.info('')
                    logger.info(f'  Cleaning up some stale workers of flavor {job_type.WORKER_FLAVOR_NAME}...')

                    # Destroy one at a time
                    stale_worker = filtered_stale_workers[0]
                    delete_worker_with_openstack(logger, self.openstack_client, stale_worker)


WorkerPoolManagement.start()
