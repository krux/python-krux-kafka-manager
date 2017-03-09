# -*- coding: utf-8 -*-
#
# © 2016 Krux Digital, Inc.
#

#
# Standard libraries
#

from __future__ import absolute_import
import unittest

#
# Third party libraries
#

from mock import MagicMock, patch

#
# Internal libraries
#

from krux_kafka_manager.kafka_manager_api import KafkaManager, get_kafka_manager_api, NAME, KafkaManagerApiError


class GetKafkaManagerTest(unittest.TestCase):
    FAKE_HOSTNAME = 'fake.krxd.net'
    FAKE_USE_SSL = False

    @patch('krux_kafka_manager.kafka_manager_api.KafkaManager')
    def test_get_kafka_manager_api_all_inputs(self, mock_kafka_manager):
        """
        get_kafka_manager_api() correctly initializes KafkaManager object if all parameters are given
        """
        args = MagicMock(hostname=self.FAKE_HOSTNAME, use_ssl=self.FAKE_USE_SSL)
        logger = MagicMock()
        stats = MagicMock()

        get_kafka_manager_api(args, logger, stats)

        mock_kafka_manager.assert_called_once_with(
            hostname=GetKafkaManagerTest.FAKE_HOSTNAME,
            use_ssl=GetKafkaManagerTest.FAKE_USE_SSL,
            logger=logger,
            stats=stats,
        )

    @patch('krux_kafka_manager.kafka_manager_api.get_stats')
    @patch('krux_kafka_manager.kafka_manager_api.get_logger')
    @patch('krux_kafka_manager.kafka_manager_api.get_parser')
    @patch('krux_kafka_manager.kafka_manager_api.add_kafka_manager_api_cli_arguments')
    @patch('krux_kafka_manager.kafka_manager_api.KafkaManager')
    def test_get_kafka_api_no_inputs(self, mock_kafka_manager, mock_cli_args, mock_parser, mock_logger, mock_stats):
        """
        get_kafka_manager_api() correctly initializes KafkaManager object if no parameters are given
        """
        mock_parser.return_value.parse_args.return_value = MagicMock(
            hostname=GetKafkaManagerTest.FAKE_HOSTNAME,
            use_ssl=GetKafkaManagerTest.FAKE_USE_SSL,
        )

        get_kafka_manager_api()

        mock_parser.assert_called_once_with(description=NAME)
        mock_cli_args.assert_called_once_with(mock_parser.return_value)
        mock_logger.assert_called_once_with(name=NAME)
        mock_stats.assert_called_once_with(prefix=NAME)

        mock_kafka_manager.assert_called_once_with(
            hostname=GetKafkaManagerTest.FAKE_HOSTNAME,
            use_ssl=GetKafkaManagerTest.FAKE_USE_SSL,
            logger=mock_logger.return_value,
            stats=mock_stats.return_value,
        )


class KafkaManagerTest(unittest.TestCase):

    HOSTNAME = 'test_hostname'
    USE_SSL = False
    PROTOCOL = 'https' if USE_SSL else 'http'
    CLUSTER = 'cluster_name'
    TOPIC = 'topic_name'
    STATUS = 'active'
    ACTIVE_CLUSTERS = [{'name': CLUSTER}]
    PENDING_CLUSTERS = [{'name': 'pending_cluster'}]
    CLUSTERS_PER_STATUS = {STATUS: ACTIVE_CLUSTERS, 'pending': PENDING_CLUSTERS}
    TOPIC_IDENTITIES = [{'topic': TOPIC, 'partitionsIdentity': [{'partNum': 0}, {'partNum': 1}, {'partNum': 2}]}]

    TEST_ERROR_CODE = 500
    TEST_FAILURE = 'Internal Server Error'
    TEST_FAILURE_BODY = {
        'error': 'This is a drill'
    }

    def setUp(self):
        self._logger = MagicMock()
        self._stats = MagicMock()
        self._manager = KafkaManager(
            hostname=KafkaManagerTest.HOSTNAME,
            use_ssl=KafkaManagerTest.USE_SSL,
            logger=self._logger,
            stats=self._stats
        )

    def test_KafkaManagerAPI_all_init(self):
        """
        KafkaManager.__init__() correctly initializes properties if all parameters are given
        """
        self.assertEqual(NAME, self._manager._name)
        self.assertEqual(self._logger, self._manager._logger)
        self.assertEqual(self._stats, self._manager._stats)

        self.assertEqual(KafkaManagerTest.HOSTNAME, self._manager._hostname)
        self.assertEqual('http', self._manager._protocol)

    @patch('krux_kafka_manager.kafka_manager_api.get_logger')
    @patch('krux_kafka_manager.kafka_manager_api.get_stats')
    def test_KafkaManagerAPI_only_hostname(self, mock_stats, mock_logger):
        """
        KafkaManager.__init__() correctly initializes properties if only mandatory parameters are given
        """
        manager = KafkaManager(
            hostname=KafkaManagerTest.HOSTNAME,
        )
        mock_logger.assert_called_once_with(NAME)
        mock_stats.assert_called_once_with(prefix=NAME)
        self.assertEqual(mock_logger.return_value, manager._logger)
        self.assertEqual(mock_stats.return_value, manager._stats)
        self.assertEqual('https', manager._protocol)

    @patch('krux_kafka_manager.kafka_manager_api.requests')
    def test_request_failure(self, mock_requests):
        """
        KafkaManager._call() correctly handles request failure
        """
        mock_requests.request.return_value = MagicMock(
            status_code=KafkaManagerTest.TEST_ERROR_CODE,
            reason=KafkaManagerTest.TEST_FAILURE,
            content=KafkaManagerTest.TEST_FAILURE_BODY,
        )

        with self.assertRaises(KafkaManagerApiError) as e:
            self._manager.get_cluster_list()

        self.assertEqual(
            '{status_code} {reason} was returned. Body: {body}'.format(
                status_code=KafkaManagerTest.TEST_ERROR_CODE,
                reason=KafkaManagerTest.TEST_FAILURE,
                body=KafkaManagerTest.TEST_FAILURE_BODY,
            ),
            str(e.exception)
        )

    @patch('krux_kafka_manager.kafka_manager_api.requests')
    def test_get_cluster_list(self, mock_requests):
        """
        KafkaManager.get_cluster_list() correctly returns list of clusters for hostname.
        """
        mock_requests.request.return_value = MagicMock(
            status_code=200,
            json=MagicMock(
                return_value={
                    'clusters': KafkaManagerTest.CLUSTERS_PER_STATUS,
                },
            ),
        )

        cluster_list = self._manager.get_cluster_list()

        mock_requests.request.assert_called_once_with(
            method='GET',
            url='{protocol}://{hostname}/api/status/clusters'.format(
                hostname=KafkaManagerTest.HOSTNAME,
                protocol=KafkaManagerTest.PROTOCOL,
            )
        )
        self.assertEqual(cluster_list, KafkaManagerTest.CLUSTERS_PER_STATUS)

    @patch('krux_kafka_manager.kafka_manager_api.requests')
    def test_get_cluster_list_status(self, mock_requests):
        """
        KafkaManager.get_cluster_list() correctly returns list of clusters for hostname.
        """
        mock_requests.request.return_value = MagicMock(
            status_code=200,
            json=MagicMock(
                return_value={
                    'clusters': KafkaManagerTest.CLUSTERS_PER_STATUS,
                },
            ),
        )

        cluster_list = self._manager.get_cluster_list(status=KafkaManagerTest.STATUS)

        self.assertEqual(cluster_list, KafkaManagerTest.ACTIVE_CLUSTERS)

    @patch('krux_kafka_manager.kafka_manager_api.requests')
    def test_get_topic_identities(self, mock_requests):
        """
        KafkaManager.get_topic_identities() method correctly returns topic identities for all topics for given cluster
        """
        mock_requests.request.return_value = MagicMock(
            status_code=200,
            json=MagicMock(
                return_value={
                    'topicIdentities': KafkaManagerTest.TOPIC_IDENTITIES,
                },
            ),
        )

        topic_identities = self._manager.get_topic_identities(KafkaManagerTest.CLUSTER)

        mock_requests.request.assert_called_once_with(
            method='GET',
            url='{protocol}://{hostname}/api/status/{cluster}/topicIdentities'.format(
                hostname=KafkaManagerTest.HOSTNAME,
                protocol=KafkaManagerTest.PROTOCOL,
                cluster=KafkaManagerTest.CLUSTER
            )
        )

        self.assertEqual(topic_identities, KafkaManagerTest.TOPIC_IDENTITIES)
