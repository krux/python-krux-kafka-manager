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

    @patch('krux_kafka_manager.kafka_manager_api.get_stats')
    @patch('krux_kafka_manager.kafka_manager_api.get_logger')
    def setUp(self, mock_logger, mock_stats):
        self.mock_logger = mock_logger
        self.mock_stats = mock_stats
        self.manager = KafkaManagerAPI("test_hostname")
    
    def test_KafkaManagerAPI_all_init(self):
        """
        Checks if KafkaManagerAPI initialized property if all user inputs provided
        """
        manager = KafkaManagerAPI("test_hostname", self.mock_logger, self.mock_stats)
        self.assertIn('test_hostname', manager._hostname)
        self.assertIn(NAME, manager._name)
        self.assertEqual(self.mock_logger, manager._logger)
        self.assertEqual(self.mock_stats, manager._stats)

    def test_KafkaManagerAPI_only_hostname(self):
        """
        Checks if KafkaManagerAPI initialized properly with no user inputs except (required) hostname
        """    
        self.mock_logger.assert_called_once_with(self.manager._name)
        self.mock_stats.assert_called_once_with(prefix=self.manager._name)

    @patch('krux_kafka_manager.kafka_manager_api.KafkaManagerAPI')
    def test_get_kafka_manager_api_all_inputs(self, mock_kafka_manager):
        """
        Checks if get_kafka_manager_api initalizes KafkaManagerAPI object with all user inputs provided
        (except mandatory hostname argument)
        """
        mock_kafka_manager.return_value = MagicMock()
        mock_args = MagicMock(hostname='test_hostname')
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
        Checks if get_kafka_manager_api initalizes KafkaManagerAPI object with no user inputs provided
        (except mandatory hostname argument)
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
    def test_get_brokers_skew(self, mock_requests):
        """
        Checks if get_brokers_skew method correctly returns brokersSkewPercentage for given cluster and topic
        """
        mock_requests.get.return_value.json.return_value = {'topics':'test','brokersSkewPercentage':0}
        test_case = KafkaManagerAPI('hostname')
        brokers_skew = test_case.get_brokers_skew('cluster', 'test')
        self.assertEqual(brokers_skew, 0)
