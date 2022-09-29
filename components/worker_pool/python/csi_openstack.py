'''
_summary_

:raises CsiExternalError: _description_
:raises CsiExternalError: _description_
:raises CsiExternalError: _description_
:return: _description_
:rtype: _type_
'''
import datetime

import keystoneauth1
import openstack

from ...common.python.database.logger import Logger
from ...common.python.util.exceptions import CsiExternalError
from ...common.python.util.datetime_util import DatetimeUtil


def get_openstack_instance_age(instance):
    '''
    _summary_

    :param instance: _description_
    :type instance: _type_
    :return: _description_
    :rtype: _type_
    '''
    updated_date = DatetimeUtil.fromRfc3339(instance.updated)
    age = datetime.datetime.utcnow() - updated_date.replace(tzinfo=None)
    return age


def ask_openstack_to_create_worker(logger: Logger,
                                   worker_name: str,
                                   flavor_name: str,
                                   openstack_client: openstack.connect):
    logger.info('   extracting os-worker template')
    worker_template_server_name = 'tf-titi'
    server = openstack_client.get_server(worker_template_server_name)
    if server is None:
        raise CsiExternalError(
            'Missing worker template instance',
            f'Can\'t find the OpenStack instance for the worker template '
            f'which name is "{worker_template_server_name}"'
            )


    image_template_prefix = 'os-worker-template-'
    worker_template_image_name = f'{image_template_prefix}{server.id[:8]}'
    logger.info(f'      - looking for image name "{worker_template_image_name}"')

    images = openstack_client.compute.images()
    for image in images:
        if (
            image.name.startswith(image_template_prefix)
            and image.name != worker_template_image_name
        ):
            logger.info(
                f'      - found an older worker template OpenStack image '
                f'("{image.name}"), remove it...'
                )
            openstack_client.compute.delete_image(image)
    image = openstack_client.compute.find_image(worker_template_image_name)
    if image is None:
        logger.info(
            '      worker template image not found, create it from the worker '
            'template instance...'
            )
        console = openstack_client.compute.get_server_console_output(server)
        init_success_key = 'init_instance_finished_with_success'
        if init_success_key in console['output']:
            image = openstack_client.compute.create_server_image(
                server, worker_template_image_name, wait=True)
        else:
            reasonnable_init_max_duration = datetime.timedelta(minutes=10)
            age = get_openstack_instance_age(server)
            if age < reasonnable_init_max_duration:
                logger.info(
                    '       worker template initialisation is not finished, please wait '
                    'a few moment and try again')
                return 'creation_request_aborted'
            raise CsiExternalError(
                'Worker template frozen',
                'It seems that the instance for the worker template has '
                'not successfully finished its initialisation in a '
                'reasonnable time, please check its console output to see '
                'if there was a problem'
                )


    logger.debug(f'     - worker template image id = {image.id}')

    openstack_client.compute.fetch_server_security_groups(server)
    networks_names = server.addresses.keys()
    networks = [
        openstack_client.network.find_network(network_name)
        for network_name in networks_names
    ]
    networkds_uuids = [
        { 'uuid': network.id }
        for network in networks
    ]

    try:
        logger.info('      - actually execute the OpenStack instance creation')
        openstack_client.compute.create_server(
            name=worker_name,
            image_id=image.id,
            flavor_id=get_openstack_flavor_id(openstack_client, flavor_name),
            networks=networkds_uuids,
            security_groups=server.security_groups,
            key_name=server.key_name
            )
        openstack_client.compute.wait_for_server(server)
    except openstack.exceptions.HttpException as error:
        raise CsiExternalError(
            'Worker creation OpenStack error',
            'An HTTP error occured during the creation of the worker '
            f'creation: "{error}"'
            ) from error
    return 'creation_request_submitted'


def delete_worker_with_openstack(logger: Logger, openstack_client, worker: dict):
    worker_openstack_id = worker['openstack'].id
    name = worker['name']
    ip_address = worker['ip']
    nomad_id = worker['nomad']['ID'] if worker['nomad'] is not None else '<no Nomad node>'
    flavor_name = worker['openstack']['flavor']['original_name']
    logger.info(
        f'  asking OpenStack to delete worker: name = {name}, IP = {ip_address}, ID = '
        f'{worker_openstack_id}, Nomad ID = {nomad_id}, flavor = {flavor_name}'
        )
    try:
        openstack_client.delete_server(worker_openstack_id, wait=True)
    except openstack.exceptions.HttpException as error:
        logger.error(
            f'HTTP error {error.response.status_code} while deleting worker '
            f'{name} with OpenStack: {error}'
            )
    except openstack.exceptions.ResourceTimeout as error:
        logger.error(
            f'OpenStack connection timeout error while deleting worker {name}: '
            f'{error}'
            )
    except openstack.exceptions.SDKException as error:
        # We would have prefer to not catch this high level exception (I guess
        # SDK exception can happens for some other reasons than connection 
        # errors) but this is raise with come connection errors like
        # keystoneauth1.exceptions.connection.ConnectFailure, so we have to
        # catch it.
        logger.error(
            f'OpenStack connection error while deleting worker {name}: '
            f'{error}'
            )
    except keystoneauth1.exceptions.connection.ConnectFailure as error:
        logger.error(
            f'OpenStack connection error while deleting worker '
            f'{name}. '
            f'keystoneauth1.exceptions.connection.ConnectFailure: '
            f'{error}'
            )
    logger.info('   Done')

def get_openstack_flavor_id(openstack_client, flavor_name: str):
    flavor = openstack_client.compute.find_flavor(flavor_name)
    return flavor.id

def get_openstack_flavor_vcpus(openstack_client, flavor_name: str):
    flavor = openstack_client.compute.get_flavor(get_openstack_flavor_id(openstack_client, flavor_name))
    return flavor.vcpus
