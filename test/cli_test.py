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

from mock import MagicMock, patch, call

#
# Internal libraries
#

from krux.stats import DummyStatsClient
from krux_kafka_manager.cli import Application, NAME, main


class CLItest(unittest.TestCase):

    @patch('krux_kafka_manager.cli.krux.cli.krux.logging.get_logger')
    @patch('krux_kafka_manager.cli.get_kafka_manager_api')
    @patch('sys.argv', ['krux-kafka', 'http://localhost:9000'])
    def setUp(self, mock_get_manager, mock_get_logger):
        self.app = Application()
        self.mock_get_manager = mock_get_manager
        self.mock_get_logger = mock_get_logger

    def test_init(self):
        """
        CLI Test: CLI constructor creates all the required private properties
        """
        # There are not much we can test except all the objects are under the correct name
        self.assertEqual(NAME, self.app.name)
        self.assertEqual(NAME, self.app.parser.description)
        # The dummy stats client has no awareness of the name. Just check the class.
        self.assertIsInstance(self.app.stats, DummyStatsClient)

        self.mock_get_manager.assert_called_once_with(
            args=self.app.args,
            logger=self.app.logger,
            stats=self.app.stats
        )

    def test_add_cli_arguments(self):
        """
        CLI Test: All arguments from Kafka Manager API are present in the args
        """
        self.assertIn('hostname', self.app.args)

    def test_run(self):
        """
        CLI Test: Kafka Manager API's get_cluster_list method is correctly called in self.app.run()
        """
        self.app.run()
        self.mock_get_manager().get_cluster_list.assert_called_once_with()

    def test_main(self):
        """
        CLI Test: Application is instantiated and run() is called in main()
        """
        app = MagicMock()
        app_class = MagicMock(return_value=app)

        with patch('krux_kafka_manager.cli.Application', app_class):
            main()

        app_class.assert_called_once_with()
        app.run.assert_called_once_with()
