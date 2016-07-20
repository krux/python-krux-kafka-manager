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

#
# Third party libraries
#

#
# Internal libraries
#

import krux.cli
from krux.logging import get_logger
from krux.cli import get_group
from krux_kafka_manager.kafka_manager import NAME, KafkaManager, get_kafka_manager, add_kafka_manager_cli_arguments


class Application(krux.cli.Application):
    def __init__(self, name=NAME):
        # Call to the superclass to bootstrap.
        super(Application, self).__init__(name=name)

        self.logger = get_logger(name)

        self.kafka_manager = get_kafka_manager(args=self.args, logger=self.logger, stats=self.stats)

    def add_cli_arguments(self, parser):
        """
        Add KafkaManager-related command-line arguments to the given parser.

        :argument parser: parser instance to which the arguments will be added
        """
        #super(Application, self).add_cli_arguments(parser)

        add_kafka_manager_cli_arguments(parser)

        group = get_group(parser, self.name)

        group.add_argument(
            "-c",
            type=str,
            help="Kafka cluster name.",
        )

        group.add_argument(
            "-t",
            type=str,
            help="Kafka topic name.",
        )

    def run(self):
        get_brokers_skew = self.kafka_manager.get_brokers_skew(self.args.cluster, self.args.topic)
        self.logger.debug(get_brokers_skew)


def main():
    app = Application()
    with app.context():
        app.run()

# Run the application stand alone
if __name__ == '__main__':
    main()

