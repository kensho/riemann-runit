#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This file is part of riemann-runit.
# https://github.com/kensho/riemann-runit

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2015, Ashok Rao <ops@kensho.com>

from setuptools import setup, find_packages
from riemann_runit import __version__
from pip.req import parse_requirements
try:
    from pip.download import PipSession
    install_reqs = parse_requirements('riemann_runit/requirements.txt', session=PipSession())
except ImportError:
    install_reqs = parse_requirements('riemann_runit/requirements.txt')


# reqs is a list of requirement
reqs = [str(ir.req) for ir in install_reqs]


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
    install_requires=reqs,
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
