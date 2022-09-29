'''
_summary_

:return: _description_
:rtype: _type_
'''
import datetime


def to_python_timestamp(nomad_timestamp):
    '''
    _summary_

    :param nomad_timestamp: _description_
    :type nomad_timestamp: _type_
    :return: _description_
    :rtype: _type_
    '''
    time_in_seconds = nomad_timestamp/(1000000*1000)
    return datetime.datetime.fromtimestamp(time_in_seconds)


def get_time_since_last_alloc_activity(alloc):
    '''
    _summary_

    :param alloc: _description_
    :type alloc: _type_
    :return: _description_
    :rtype: _type_
    '''
    now = datetime.datetime.utcnow()
    modified = to_python_timestamp(alloc['ModifyTime'])
    time = now - modified
    return time


def get_unfinished_allocs(allocs):
    '''
    _summary_

    :param allocs: _description_
    :type allocs: _type_
    :return: _description_
    :rtype: _type_
    '''
    unfinished_allocs = [
        alloc
        for alloc in allocs
        if (
            alloc['ClientStatus'] == 'pending'
            or alloc['ClientStatus'] == 'running'
        )
    ]
    return unfinished_allocs


def get_finished_allocs(allocs):
    '''
    _summary_

    :param allocs: _description_
    :type allocs: _type_
    :return: _description_
    :rtype: _type_
    '''
    finished_allocs = [
        alloc
        for alloc in allocs
        if (
            alloc['ClientStatus'] != 'pending'
            and alloc['ClientStatus'] != 'running'
        )
    ]
    return finished_allocs


def node_hasnt_run_an_alloc_recently(nomad_client, node):
    '''
    _summary_

    :param nomad_client: _description_
    :type nomad_client: _type_
    :param node: _description_
    :type node: _type_
    :return: _description_
    :rtype: _type_
    '''
    allocs = nomad_client.node.get_allocations(node['ID'])

    unfinished_allocs = get_unfinished_allocs(allocs)

    finished_allocs = get_finished_allocs(allocs)
    sleeping_time_threshold = datetime.timedelta(minutes=2)
    recently_finished_allocs = [
        alloc
        for alloc in finished_allocs
        if get_time_since_last_alloc_activity(alloc) < sleeping_time_threshold
    ]

    node_is_sleeping = (
        len(unfinished_allocs) == 0
        and len(recently_finished_allocs) == 0
    )

    return node_is_sleeping


def there_is_no_unfinished_alloc_in_node(nomad_client, node):
    '''
    _summary_

    :param nomad_client: _description_
    :type nomad_client: _type_
    :param node: _description_
    :type node: _type_
    :return: _description_
    :rtype: _type_
    '''
    unfinished_allocs = get_unfinished_allocs(
        nomad_client.node.get_allocations(node['ID'])
        )
    return len(unfinished_allocs) == 0


def get_worker_alive_nomad_nodes(nomad_client):
    '''
    _summary_

    :param nomad_client: _description_
    :type nomad_client: _type_
    :return: _description_
    :rtype: _type_
    '''
    nodes = nomad_client.nodes.get_nodes()

    worker_nodes = [
        node
        for node in nodes
        if (
            node['Name'].startswith('os-worker')
            and node['Status'] != 'down'
        )
    ]

    return worker_nodes


def get_pending_nomad_jobs(nomad_client, job_types_list):
    '''
    _summary_

    :param nomad_client: _description_
    :type nomad_client: _type_
    :param job_types_list: _description_
    :type job_types_list: _type_
    :return: _description_
    :rtype: _type_
    '''
    nomad_jobs = nomad_client.jobs.get_jobs()

    pending_nomad_jobs = {}

    for job_type in job_types_list:
        pending_nomad_jobs[job_type] = []

    # Select only jobs matching one regex, and with a 'pending' status
    for nomad_job in nomad_jobs:
        for job_type in job_types_list:

            # Regex identifying Nomad jobs related to the specified database type job.
            nomad_job_regex = f"{job_type.NOMAD_JOB_NAME}/dispatch"

            if (nomad_job['ID'].startswith(nomad_job_regex)
                and nomad_job['Status'] == 'pending'
                and nomad_job not in pending_nomad_jobs[job_type]):

                pending_nomad_jobs[job_type].append(nomad_job)

    return pending_nomad_jobs


def get_ready_and_unallocated_nomad_nodes(nomad_client):
    nomad_nodes = nomad_client.nodes.get_nodes()

    ready_and_unallocated_nomad_nodes = [
        node
        for node in nomad_nodes
        if (
            node['Status'] == 'ready'
            and there_is_no_unfinished_alloc_in_node(nomad_client, node)
            )
    ]
    return ready_and_unallocated_nomad_nodes
