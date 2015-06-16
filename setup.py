#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This file is part of riemann-runit.
# https://github.com/kensho/riemann-runit

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2015, Ashok Rao <ops@kensho.com>

from setuptools import setup, find_packages

from riemann_runit import __version__


tests_require = [
    'mock',
    'nose',
    'coverage',
    'yanc',
    'preggy',
    'tox',
    'ipdb',
    'coveralls',
    'sphinx',
]

setup(
    name='riemann-runit',
    version=__version__,
    description='Riemann runit collector',
    long_description='''
Riemann runit collector
''',
    keywords='',
    author='Ashok Rao',
    author_email='ops@kensho.com',
    url='https://github.com/kensho/riemann-runit',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Programming Language :: Python :: 2.7',
        'Operating System :: OS Independent',
    ],
    packages=find_packages(),
    include_package_data=False,
    install_requires=[
        'bernhard==0.2.3',
        'click==3.3',
        'protobuf==2.6.1',
        'riemann-client==6.0.2'
    ],
    extras_require={
        'tests': tests_require,
    },
    entry_points={
        'console_scripts': [
            # add cli scripts here in this form:
            'riemann-runit=riemann_runit.main:main_cli',
        ],
    },
)
