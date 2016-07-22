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
# Internal libraries
#

import krux.cli
from krux_kafka_manager.kafka_manager_api import NAME, KafkaManagerAPI, get_kafka_manager_api, add_kafka_manager_api_cli_arguments


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
        get_brokers_skew = self.kafka_manager_api.get_brokers_skew('krux-manager-test', 'test')
        self.logger.info(get_brokers_skew)

        get_topic_list = self.kafka_manager_api.get_topic_list('krux-manager-test')
        self.logger.info(get_topic_list)

        all_clusters = self.kafka_manager_api.get_cluster_list()
        self.logger.info(all_clusters)

        active_clusters = self.kafka_manager_api.get_cluster_list(params={"status": "active"})
        self.logger.info(active_clusters)

        pending_clusters = self.kafka_manager_api.get_cluster_list(params={"status": "pending"})
        self.logger.info(pending_clusters)

        # for cluster in all_clusters:
        #     for topic in get_topic_list:
        #         self.logger.info('Cluster {cluster} - Topic {topic} - Broker Skew Percentage: {skew}'.format(
        #                 cluster=cluster['name'],
        #                 topic=topic,
        #                 skew=self.kafka_manager_api.get_brokers_skew(cluster['name'], topic)
        #             ))


def main():
    app = Application()
    with app.context():
        app.run()

# Run the application stand alone
if __name__ == '__main__':
    main()
