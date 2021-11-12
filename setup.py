#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing  # To make python setup.py test happy
import os
import shutil
import subprocess

from distutils.command.clean import clean
from setuptools import find_packages
from setuptools import setup

multiprocessing

PACKAGE = 'Cron-o'
__version__ = None


setup(
    name=PACKAGE,
    version=__version__,
    description='Cron-o',
    long_description=open('README.md').read(),
    author='Gabriel Slomka',
    author_email='gabrielslomka@gmail.com',
    license='Apache License, Version 2',
    keywords='distributed cron job scheduler',
    packages=find_packages(),
    include_package_data=True,
    extras_require={'python_version<"3.3"': ['funcsigs']},
    tests_require=[
        'funcsigs',
    ],
    test_suite='nose.collector',
    install_requires=[
        'aioredis',
        'pyrobuf'
    ],
)
