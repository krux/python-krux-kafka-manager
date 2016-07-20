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


def get_kafka_manager(args=None, logger=None, stats=None): # TODO
    """
    Return a usable Kafka Manager object without creating a class around it.
    In the context of a krux.cli (or similar) interface the 'args', 'logger'
    and 'stats' objects should already be present. If you don't have them,
    however, we'll attempt to provide usable ones.
    """
    if not args:
        parser = get_parser(description=NAME)
        add_kafka_manager_cli_arguments(parser)
        args = parser.parse_args()

    if not logger:
        logger = get_logger(name=NAME)

    if not stats:
        stats = get_stats(prefix=NAME)

    return KafkaManager(
        hostname=args.hostname,
        logger=logger,
        stats=stats,
    )


def add_kafka_manager_cli_arguments(parser):
    """
    Utility function for adding Kafka Manager specific CLI arguments.
    """
    # Add those specific to the application
    group = get_group(parser, NAME)

    group.add_argument(
        "-H, --hostname",
        type=str,
        default="kafka-manager.krxd.net",
        help="Kafka Manager hostname. (default: %(default)s)",
    )


class KafkaManager(object):
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

    def get_brokers_skew(self, cluster, topic):
        r = requests.get('%s/api/status/%s/%s/brokersSkewPercentage' % (self._hostname, cluster, topic))
        return r.json()['brokersSkewPercentage']
