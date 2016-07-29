# -*- coding: utf-8 -*-
#
# © 2016 Krux Digital, Inc.
#

#
# Standard libraries
#

from __future__ import absolute_import
import unittest
import sys

#
# Third party libraries
#

from mock import MagicMock, patch

#
# Internal libraries
#

from krux_kafka_manager.kafka_manager_api import KafkaManagerAPI, get_kafka_manager_api, NAME


class KafkaManagerTest(unittest.TestCase):

    _HOSTNAME = 'test_hostname'

    @patch('krux_kafka_manager.kafka_manager_api.get_stats')
    @patch('krux_kafka_manager.kafka_manager_api.get_logger')
    def setUp(self, mock_logger, mock_stats):
        self.mock_logger = mock_logger
        self.mock_stats = mock_stats
        self.manager = KafkaManagerAPI(hostname=KafkaManagerTest._HOSTNAME)

    def test_KafkaManagerAPI_all_init(self):
        """
        Kafka Manager API Test: Checks if KafkaManagerAPI initialized property if all user
        inputs provided.
        """
        manager = KafkaManagerAPI(KafkaManagerTest._HOSTNAME, self.mock_logger, self.mock_stats)
        self.assertIn(KafkaManagerTest._HOSTNAME, manager._hostname)
        self.assertIn(NAME, manager._name)
        self.assertEqual(self.mock_logger, manager._logger)
        self.assertEqual(self.mock_stats, manager._stats)

    def test_KafkaManagerAPI_only_hostname(self):
        """
        Kafka Manager API Test: Checks if KafkaManagerAPI initialized properly with no user 
        inputs except (required) hostname.
        """
        self.mock_logger.assert_called_once_with(self.manager._name)
        self.mock_stats.assert_called_once_with(prefix=self.manager._name)

    @patch('krux_kafka_manager.kafka_manager_api.KafkaManagerAPI')
    def test_get_kafka_manager_api_all_inputs(self, mock_kafka_manager):
        """
        Checks if get_kafka_manager_api initalizes KafkaManagerAPI object with all user inputs
        provided (except mandatory hostname argument).
        """
        mock_kafka_manager.return_value = MagicMock()
        mock_args = MagicMock(hostname=KafkaManagerTest._HOSTNAME)
        manager = get_kafka_manager_api(mock_args, self.mock_logger, self.mock_stats)
        mock_kafka_manager.assert_called_once_with(
                hostname='test_hostname',
                logger=self.mock_logger,
                stats=self.mock_stats,
                )

    @patch('krux_kafka_manager.kafka_manager_api.get_stats')
    @patch('krux_kafka_manager.kafka_manager_api.get_logger')
    @patch('krux_kafka_manager.kafka_manager_api.get_parser')
    @patch('krux_kafka_manager.kafka_manager_api.add_kafka_manager_api_cli_arguments')
    @patch('krux_kafka_manager.kafka_manager_api.KafkaManagerAPI')
    def test_get_kafka_api_no_inputs(self, mock_kafka_manager, mock_cli_args, mock_parser, mock_logger, mock_stats):
        """
        Kafka Manager API Test: Checks if get_kafka_manager_api initalizes KafkaManagerAPI object
        with no user inputs provided (except mandatory hostname argument)
        """
        mock_parser.return_value.parse_args.return_value = MagicMock(hostname=KafkaManagerTest._HOSTNAME)
        manager = get_kafka_manager_api()
        mock_cli_args.assert_called_once_with(mock_parser(description=NAME))
        mock_logger.assert_called_once_with(name=NAME)
        mock_stats.assert_called_once_with(prefix=NAME)
        mock_kafka_manager.assert_called_once_with(
            hostname = KafkaManagerTest._HOSTNAME,
            logger=mock_logger(name=NAME),
            stats=mock_stats(prefix=NAME),
            )

    @patch('krux_kafka_manager.kafka_manager_api.get_stats')
    @patch('krux_kafka_manager.kafka_manager_api.get_logger')
    @patch('krux_kafka_manager.kafka_manager_api.get_parser')
    @patch('krux_kafka_manager.kafka_manager_api.add_kafka_manager_api_cli_arguments')
    @patch('krux_kafka_manager.kafka_manager_api.KafkaManagerAPI')
    def test_get_kafka_api_no_inputs(self, mock_kafka_manager, mock_cli_args, mock_parser, mock_logger, mock_stats):
        """
        Kafka Manager API Test: Checks if get_kafka_manager_api initalizes KafkaManagerAPI object
        with no user inputs provided (except mandatory hostname argument)
        """
        mock_parser.return_value.parse_args.return_value = MagicMock(hostname='test_hostname')
        mock_kafka_manager.return_value = MagicMock()
        manager = get_kafka_manager_api()
        mock_cli_args.assert_called_once_with(mock_parser(description=NAME))
        mock_kafka_manager.assert_called_once_with(
            hostname = 'test_hostname',
            logger=mock_logger(name=NAME),
            stats=mock_stats(prefix=NAME),
            )

    @patch('krux_kafka_manager.kafka_manager_api.requests')
    def test_get_cluster_list(self, mock_requests):
        """
        Kafka Manager API Test: Checks if get_cluster_list method correctly returns list of clusters for hostname.
        """
        mock_requests.get.return_value.json.return_value = {'clusters':[{'name': 'cluster1', 'status': 'active'}]}
        cluster_list = self.manager.get_cluster_list('params')
        mock_requests.get.assert_called_once_with('{hostname}/api/status/clusters'.format(
            hostname=KafkaManagerTest._HOSTNAME), params='params')
        self.assertEqual(cluster_list, [{'name': 'cluster1', 'status': 'active'}])

    @patch('krux_kafka_manager.kafka_manager_api.requests')
    def test_get_topic_list(self, mock_requests):
        """
        Kafka Manager API Test: Checks if get_topic_list method correctly returns list of topic names for
        given cluster.
        """
        mock_requests.get.return_value.json.return_value = {'topics': ['topic1', 'topic2']}
        topic_list = self.manager.get_topic_list('cluster')
        mock_requests.get.assert_called_once_with('{hostname}/api/status/{cluster}/topics'.format(
            hostname=KafkaManagerTest._HOSTNAME,
            cluster='cluster'))
        self.assertEqual(topic_list, ['topic1', 'topic2'])

    @patch('krux_kafka_manager.kafka_manager_api.requests')
    def test_get_topic_identities(self, mock_requests):
        """
        Kafka Manager API Test: Checks if get_topic_identities method correctly returns topic identities
        for all topics for given cluster.
        """
        mock_requests.get.return_value.json.return_value = {'topicIdentities':[{'topic':'topic_name','partitions':50,'numBrokers':1}]}
        topic_identities = self.manager.get_topic_identities('cluster')
        mock_requests.get.assert_called_once_with('{hostname}/api/status/{cluster}/topicIdentities'.format(
            hostname=KafkaManagerTest._HOSTNAME,
            cluster='cluster'))
        self.assertEqual(topic_identities, [{'topic':'topic_name','partitions':50,'numBrokers':1}])

    @patch('krux_kafka_manager.kafka_manager_api.requests')
    def test_get_brokers_skew(self, mock_requests):
        """
        Kafka Manager API Test: Checks if get_brokers_skew method correctly returns brokersSkewPercentage
        for given cluster and topic
        """
        mock_requests.get.return_value.json.return_value = {'topics':'test','brokersSkewPercentage':0}
        brokers_skew = self.manager.get_brokers_skew('cluster', 'test')
        mock_requests.get.assert_called_once_with('{hostname}/api/status/{cluster}/{topic}/brokersSkewPercentage'.format(
            hostname=KafkaManagerTest._HOSTNAME,
            cluster='cluster',
            topic='test'))
        self.assertEqual(brokers_skew, 0)
