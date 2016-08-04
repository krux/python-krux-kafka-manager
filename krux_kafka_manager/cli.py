# -*- coding: utf-8 -*-
#
# © 2016 Krux Digital, Inc.
#
"""
CLI tools for accessing Krux Kafka Clusters
"""

#
# Standard libraries
#

from __future__ import absolute_import
from pprint import pformat

#
# Internal libraries
#

import krux.cli
from krux_kafka_manager.kafka_manager_api import NAME, get_kafka_manager_api, add_kafka_manager_api_cli_arguments


# CLI for testing purposes
class Application(krux.cli.Application):
    def __init__(self, name=NAME):
        # Call to the superclass to bootstrap.
        super(Application, self).__init__(name=name)

        self.kafka_manager_api = get_kafka_manager_api(args=self.args, logger=self.logger, stats=self.stats)

    def add_cli_arguments(self, parser):
        """
        Add KafkaManager-related command-line arguments to the given parser.

        :argument parser: parser instance to which the arguments will be added
        """
        super(Application, self).add_cli_arguments(parser)

        add_kafka_manager_api_cli_arguments(parser)
        group = krux.cli.get_group(parser, self.name)

    def run(self):
        topic_identities = self.kafka_manager_api.get_topic_identities('krux-manager-test')
        self.logger.info(pformat(topic_identities))

        get_cluster_list = self.kafka_manager_api.get_cluster_list(status='active')
        self.logger.info(pformat(get_cluster_list))

        partitions_identity = self.kafka_manager_api.get_partitions_identity('krux-manager-test', '__consumer_offsets')
        self.logger.info(pformat(partitions_identity))


def main():
    app = Application()
    with app.context():
        app.run()

# Run the application stand alone
if __name__ == '__main__':
    main()
