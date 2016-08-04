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
    CLUSTER = 'cluster'
    TOPIC = 'topic_name'
    STATUS = 'active'
    CLUSTER_LIST_REQUEST = {'clusters':[{'name': 'cluster1', 'status': 'active'}]}
    CLUSTER_LIST_STATUS = {
            'clusters': { 
                'active': [{'name': 'cluster1', 'status': 'active'}],
                'pending': [{'name': 'cluster2', 'status': 'pending'}]
            }
        }
    CLUSTER_LIST_RV =  [{'name': 'cluster1', 'status': 'active'}]
    TOPIC_IDENTITIES_REQUEST = {'topicIdentities': [{'topic':'topic_name', 'partitionsIdentity':
                        [{'partNum': 0}, {'partNum': 1}, {'partNum': 2}]}]}
    TOPIC_IDENTITIES_RV =  [{'topic':'topic_name', 'partitionsIdentity': [{'partNum': 0}, {'partNum': 1}, {'partNum': 2}]}]
    PARTITIONS_IDENTITY_RV = [{'partNum': 0}, {'partNum': 1}, {'partNum': 2}]

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
                hostname=KafkaManagerTest._HOSTNAME,
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
        mock_parser.return_value.parse_args.return_value = MagicMock(hostname=KafkaManagerTest._HOSTNAME)
        mock_kafka_manager.return_value = MagicMock()
        manager = get_kafka_manager_api()
        mock_cli_args.assert_called_once_with(mock_parser(description=NAME))
        mock_kafka_manager.assert_called_once_with(
            hostname = KafkaManagerTest._HOSTNAME,
            logger=mock_logger(name=NAME),
            stats=mock_stats(prefix=NAME),
            )

    @patch('krux_kafka_manager.kafka_manager_api.requests')
    def test_get_cluster_list(self, mock_requests):
        """
        Kafka Manager API Test: Checks if get_cluster_list method correctly returns list of clusters for hostname.
        """
        mock_requests.get.return_value.json.return_value = KafkaManagerTest.CLUSTER_LIST_REQUEST
        cluster_list = self.manager.get_cluster_list()
        mock_requests.get.assert_called_once_with('{hostname}/api/status/clusters'.format(
            hostname=KafkaManagerTest._HOSTNAME))
        self.assertEqual(cluster_list, KafkaManagerTest.CLUSTER_LIST_RV)

    @patch('krux_kafka_manager.kafka_manager_api.requests')
    def test_get_cluster_list_status(self, mock_requests):
        mock_requests.get.return_value.json.return_value = KafkaManagerTest.CLUSTER_LIST_STATUS
        cluster_list = self.manager.get_cluster_list(status=KafkaManagerTest.STATUS)
        mock_requests.get.assert_called_once_with('{hostname}/api/status/clusters'.format(
            hostname=KafkaManagerTest._HOSTNAME))
        self.assertEqual(cluster_list, KafkaManagerTest.CLUSTER_LIST_RV)

    @patch('krux_kafka_manager.kafka_manager_api.requests')
    def test_get_topic_identities(self, mock_requests):
        """
        Kafka Manager API Test: Checks if get_topic_identities method correctly returns topic identities
        for all topics for given cluster, and organizes partitionsIdentity into list of dictionaries
        ordered by partNum.
        """
        mock_requests.get.return_value.json.return_value = KafkaManagerTest.TOPIC_IDENTITIES_REQUEST
        topic_identities = self.manager.get_topic_identities(KafkaManagerTest.CLUSTER)
        mock_requests.get.assert_called_once_with('{hostname}/api/status/{cluster}/topicIdentities'.format(
            hostname=KafkaManagerTest._HOSTNAME,
            cluster=KafkaManagerTest.CLUSTER))
        self.assertEqual(topic_identities, KafkaManagerTest.TOPIC_IDENTITIES_RV)
