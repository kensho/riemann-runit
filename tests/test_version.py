#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This file is part of riemann-runit.
# https://github.com/kensho/riemann-runit

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2015, Ashok Rao <ops@kensho.com>

from preggy import expect

from riemann_runit import __version__
from tests.base import TestCase


class VersionTestCase(TestCase):
    def test_has_proper_version(self):
        expect(__version__).to_equal('0.1.0')
