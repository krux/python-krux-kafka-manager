# -*- coding: utf-8 -*-
#
# © 2016 Krux Digital, Inc.
#

######################
# Standard Libraries #
######################
from __future__ import absolute_import
from setuptools import setup, find_packages
import sys

# We use the version to construct the DOWNLOAD_URL.

VERSION = '0.2.1'

# URL to the repository on Github.
REPO_URL = 'https://github.com/krux/python-krux-kafka-manager'
# Github will generate a tarball as long as you tag your releases, so don't
# forget to tag!
DOWNLOAD_URL = ''.join((REPO_URL, '/tarball/release/', VERSION))


# python 2 will need subprocess32
dependencies = ['krux-stdlib', 'requests']
if sys.version_info[0] == 2:
    dependencies.append('subprocess32')
    dependencies.append('argparse')

# XXX these all need to be in sub dirs, or it won't work :(
setup(
    name='krux-kafka-manager',
    version=VERSION,
    author='Kelsey Lam',
    author_email='klam@krux.com',
    description='A manager for all Kafka Manager related functions',
    url=REPO_URL,
    download_url=DOWNLOAD_URL,
    license='All Rights Reserved.',
    packages=find_packages(),
    # dependencies are named in requirements.pip
    install_requires=dependencies,
    entry_points={
        'console_scripts': [
            'krux-kafka = krux_kafka_manager.cli:main',
        ],
    },
)
