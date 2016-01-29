#!/usr/bin/env python

# -*- coding: utf-8 -*-
"""

__author__ = "Oleksandr Chyrkov"
__version__ = "1.0"


This script create ElasticSearch kibana index snapshot.
In case snapshot was created successfully script rotate present
snapshots according to rotation period.

Example:
        Get help strings:

        $ kibana_backup.py --help

        Specify custom schema/port/address/rotation period or index prefix:

        $ kibana_backup.py -a <es_address> -p <es_port> -s \
                            <es_schema> -n <kibana_index_prefix>

        Run script with defaults:

        $ kibana_backup.py

"""


from elasticsearch import Elasticsearch
import datetime
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument('-a', '--address', action='store', default='elasticsearch.test.com', help='Elasticsearch address')
parser.add_argument('-p', '--port', action='store', default=9200, help='Elasticsearch port')
parser.add_argument('-P', '--period', action='store', default=5, type=int, help='Rotation period in days')
parser.add_argument('-n', '--name', action='store', default="kibana_", help='Index prefix')
parser.add_argument('-s', '--schema', action='store', default="http", help='URI schema')
args = vars(parser.parse_args())


# Create es client

es = Elasticsearch(['{0}://{1}:{2}'.format(args['schema'],
                                           args['address'],
                                           args['port'])])


def snapshot_v():

    """

    Function realize today timestamp and form snapshot name
    :return: snapshot name in form "kibana_<timestamp>"

    """

    today = datetime.datetime.utcnow().date()
    snap_name = '{0}_{1}'.format('kibana', today.strftime('%Y.%m.%d'))

    return snap_name


def create_snap():

    """

    Function creates es snap
    :return: HTTP code or trace message in case of error

    """

    snap_name = snapshot_v()

    data = {'indices': '.kibana',
            'ignore_unavailable': 'true',
            'include_global_state': 'false'}

    try:
        req = es.snapshot.create(repository='kibana',
                                 snapshot=snap_name,
                                 body=data,
                                 wait_for_completion=True)

        if req['snapshot']['state'] == 'SUCCESS':
            print('Snapshot created successfully.')

    except Exception as e:
        print(e)
        sys.exit(1)


def get_snaps_list():

    """

    Function obtains snapshots list
    :return: list of snapshots

    """

    req = es.snapshot.get(repository='kibana', snapshot='_all')

    snaps_list = [i['snapshot'] for i in req['snapshots']]

    return snaps_list


def filter_dates():

    """

    Function filter present snapshots and realize which obsolete one
    :return: list of obsolete snapshots

    """

    if len(get_snaps_list()) > args['period']:

        snaps_list = get_snaps_list()

        all_indexes = [datetime.datetime.strptime(k.split('_')[1], '%Y.%m.%d') for k in snaps_list]
        all_indexes.sort()

        obsolete_dates = [item for item in all_indexes[0:-args['period']]]

        obsolete_snaps = ['{0}{1}'.format(args['name'], i.strftime('%Y.%m.%d')) for i in obsolete_dates]

        return obsolete_snaps

    else:
        print('Snapshots count less than {}'.format(args['period']))
        sys.exit(0)


def remove_obsolete_snaps():

    """

    Function get obsolete snapshots and remove it
    :return: None

    """

    obsolete_snaps = filter_dates()

    for i in obsolete_snaps:

        try:
            req = es.snapshot.delete(repository='kibana', snapshot=i)

            if req['acknowledged']:
                print("Snapshot {0} was deleted successfully".format(i,))

        except Exception as e:
            print(e)
            sys.exit(1)


def main():

    """

    Main function call create_snap() and remove_obsolete_snaps() in above order
    :return: None

    """

    create_snap()
    remove_obsolete_snaps()


if __name__ == "__main__":
    main()
