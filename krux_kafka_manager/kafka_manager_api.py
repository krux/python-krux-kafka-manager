# -*- coding: utf-8 -*-
#
# © 2016 Krux Digital, Inc.
#

#
# Standard libraries
#

from __future__ import absolute_import

#
# Third party libraries
#
import requests

#
# Internal libraries
#

from krux.logging import get_logger
from krux.stats import get_stats
from krux.cli import get_parser, get_group

NAME = 'krux-kafka-manager'


def get_kafka_manager_api(args=None, logger=None, stats=None):
    """
    Return a usable Kafka Manager object without creating a class around it.
    In the context of a krux.cli (or similar) interface the 'args', 'logger'
    and 'stats' objects should already be present. If they are not inputted,
    we will provide usable ones.
    """
    if not args:
        parser = get_parser(description=NAME)
        add_kafka_manager_api_cli_arguments(parser)
        args = parser.parse_args()

    if not logger:
        logger = get_logger(name=NAME)

    if not stats:
        stats = get_stats(prefix=NAME)

    return KafkaManagerAPI(
        hostname=args.hostname,
        logger=logger,
        stats=stats,
    )


def add_kafka_manager_api_cli_arguments(parser):
    """
    Utility function for adding Kafka Manager specific CLI arguments.
    """
    # Add those specific to the application
    group = get_group(parser, NAME)

    group.add_argument(
        "hostname",
        type=str,
        help="Kafka Manager hostname.",
    )


class KafkaManagerAPI(object):
    """
    A manager to handle all Kafka Manager related functions.
    """
    def __init__(
        self,
        hostname,
        logger=None,
        stats=None,
    ):
        # Private variables, not to be used outside this module
        self._name = NAME
        self._logger = logger or get_logger(self._name)
        self._stats = stats or get_stats(prefix=self._name)
        self._hostname = hostname

    def get_cluster_list(self, status=None):
        """
        Returns list containing dictionaries of information for each cluster. User can filter for clusters
        with certain status, else all clusters are returned.
        """
        request_cluster_list = requests.get('{hostname}/api/status/clusters'.format(hostname=self._hostname))
        cluster_list = request_cluster_list.json()['clusters']
        if status:
            return cluster_list[status]
        return cluster_list

    def get_topic_identities(self, cluster):
        """
        Returns dictionary containing list of topic identities for given cluster. Partitions identity formatted
        into ordered list for easier use.
        """
        request_topic_identities = requests.get('{hostname}/api/status/{cluster}/topicIdentities'.format(hostname=self._hostname, cluster=cluster))
        topic_identities = request_topic_identities.json()['topicIdentities']
        return topic_identities
