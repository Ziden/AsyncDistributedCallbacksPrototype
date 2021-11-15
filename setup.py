#!/usr/bin/env python

from setuptools import find_packages
from setuptools import setup


PACKAGE = 'Cron-o'
__version__ = '0.1'


setup(
    name=PACKAGE,
    python_requires='>3.8.0',
    version=__version__,
    description='Cron-o',
    long_description=open('README.md').read(),
    author='Gabriel Slomka',
    author_email='gabrielslomka@gmail.com',
    license='Apache License, Version 2',
    keywords='distributed cron job scheduler',
    packages=find_packages(),
    include_package_data=True,
    tests_require=[
        'pytest',
        'pytest-asyncio'
    ],
    test_suite='nose.collector',
    install_requires=[
        'aioredis',
        'asyncio',
        'msgpack'
    ],
)
