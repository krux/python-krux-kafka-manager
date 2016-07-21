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

from krux.stats import DummyStatsClient
from krux_kafka_manager.cli import Application, NAME, main
from krux_kafka_manager.kafka_manager_api import KafkaManagerAPI
from krux.cli import get_group


@patch('sys.argv', ['krux-kafka', 'host'])
class CLItest(unittest.TestCase):

    def test_init(self):
        """
        CLI constructor creates all the required private properties
        """
        self.app = Application()

        # There are not much we can test except all the objects are under the correct name
        self.assertEqual(NAME, self.app.name)
        self.assertEqual(NAME, self.app.parser.description)
        # The dummy stats client has no awareness of the name. Just check the class.
        self.assertIsInstance(self.app.stats, DummyStatsClient)
        self.assertEqual(NAME, self.app.logger.name)

        self.assertIsInstance(self.app.kafka_manager_api, KafkaManagerAPI)

    @patch('krux_kafka_manager.cli.get_kafka_manager_api')
    def test_init_all_set(self, mock_getter):
        """
        Kafka Manager API is created properly.
        """
        app = Application()

        mock_getter.assert_called_once_with(
            args=app.args,
            logger=app.logger,
            stats=app.stats
        )

    def test_add_cli_arguments(self):
        """
        All arguments from Kafka Manager API are present in the args
        """
        app = Application()

        self.assertIn('hostname', app.args)

    def test_run(self):
        """
        Kafka Manager API's get_brokers_skew method is correctly called in app.run()
        """
        manager = MagicMock()

        with patch('krux_kafka_manager.cli.get_kafka_manager_api', return_value=manager):
            app = Application()
            app.run()

        manager.get_brokers_skew.assert_called_once_with('krux-manager-test', 'test')

    def test_main(self):
        """
        Application is instantiated and run() is called in main()
        """
        app = MagicMock()
        app_class = MagicMock(return_value=app)

        with patch('krux_kafka_manager.cli.Application', app_class):
            main()

        app_class.assert_called_once_with()
        app.run.assert_called_once_with()
