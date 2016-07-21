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
    def test_KafkaManagerAPI_only_hostname(self, mock_logger, mock_stats):
        app = KafkaManagerAPI("test_hostname", mock_logger, mock_stats)
        self.assertIn('test_hostname', app._hostname)
        self.assertIn(NAME, app._name)
        self.assertEqual(mock_logger, app._logger)
        self.assertEqual(mock_stats, app._stats)

    @patch('krux_kafka_manager.kafka_manager_api.get_stats')
    @patch('krux_kafka_manager.kafka_manager_api.get_logger')
    def test_KafkaManagerAPI_all_init(self, mock_logger, mock_stats):
        app = KafkaManagerAPI("test_hostname")
        mock_logger.assert_called_once_with(app._name)
        mock_stats.assert_called_once_with(prefix=app._name)


    @patch('krux_kafka_manager.kafka_manager_api.get_stats')
    @patch('krux_kafka_manager.kafka_manager_api.get_logger')
    @patch('krux_kafka_manager.kafka_manager_api.KafkaManagerAPI')
    def test_get_kafka_manager_api(self, mock_kafka_manager, mock_logger, mock_stats):
        mock_kafka_manager.return_value = MagicMock()
        mock_args = MagicMock(hostname='test_hostname')
        app = get_kafka_manager_api(mock_args)
        mock_logger.assert_called_once_with(name=NAME)
        mock_stats.assert_called_once_with(prefix=NAME)
        mock_kafka_manager.assert_called_once_with(
                hostname='test_hostname',
                logger=mock_logger(name=NAME),
                stats=mock_stats(prefix=NAME),
                )

    @patch('krux_kafka_manager.kafka_manager_api.requests')
    def test_get_brokers_skew(self, mock_requests):

        mock_requests.get.return_value.json.return_value = {'topics':'test','brokersSkewPercentage':0}
        test_case = KafkaManagerAPI('hostname')
        brokers_skew = test_case.get_brokers_skew('cluster', 'test')
        self.assertEqual(brokers_skew, 0)
