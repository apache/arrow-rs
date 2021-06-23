# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
from datetime import date, datetime
from decimal import Decimal

import arrow_pyarrow_integration_testing
import pyarrow
from pytz import timezone


class TestCase(unittest.TestCase):
    def test_primitive_python(self):
        """
        Python -> Rust -> Python
        """
        old_allocated = pyarrow.total_allocated_bytes()
        a = pyarrow.array([1, 2, 3])
        b = arrow_pyarrow_integration_testing.double(a)
        self.assertEqual(b, pyarrow.array([2, 4, 6]))
        del a
        del b
        # No leak of C++ memory
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())

    def test_primitive_rust(self):
        """
        Rust -> Python -> Rust
        """
        old_allocated = pyarrow.total_allocated_bytes()

        def double(array):
            array = array.to_pylist()
            return pyarrow.array([x * 2 if x is not None else None for x in array])

        is_correct = arrow_pyarrow_integration_testing.double_py(double)
        self.assertTrue(is_correct)
        # No leak of C++ memory
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())

    def test_string_python(self):
        """
        Python -> Rust -> Python
        """
        old_allocated = pyarrow.total_allocated_bytes()
        a = pyarrow.array(["a", None, "ccc"])
        b = arrow_pyarrow_integration_testing.substring(a, 1)
        self.assertEqual(b, pyarrow.array(["", None, "cc"]))
        del a
        del b
        # No leak of C++ memory
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())

    def test_time32_python(self):
        """
        Python -> Rust -> Python
        """
        old_allocated = pyarrow.total_allocated_bytes()
        a = pyarrow.array([None, 1, 2], pyarrow.time32("s"))
        b = arrow_pyarrow_integration_testing.concatenate(a)
        expected = pyarrow.array([None, 1, 2] + [None, 1, 2], pyarrow.time32("s"))
        self.assertEqual(b, expected)
        del a
        del b
        del expected
        # No leak of C++ memory
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())

    def test_date32_python(self):
        """
        Python -> Rust -> Python
        """
        old_allocated = pyarrow.total_allocated_bytes()
        py_array = [None, date(1990, 3, 9), date(2021, 6, 20)]
        a = pyarrow.array(py_array, pyarrow.date32())
        b = arrow_pyarrow_integration_testing.concatenate(a)
        expected = pyarrow.array(py_array + py_array, pyarrow.date32())
        self.assertEqual(b, expected)
        del a
        del b
        del expected
        # No leak of C++ memory
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())

    def test_timestamp_python(self):
        """
        Python -> Rust -> Python
        """
        old_allocated = pyarrow.total_allocated_bytes()
        py_array = [
            None,
            datetime(2021, 1, 1, 1, 1, 1, 1),
            datetime(2020, 3, 9, 1, 1, 1, 1),
        ]
        a = pyarrow.array(py_array, pyarrow.timestamp("us"))
        b = arrow_pyarrow_integration_testing.concatenate(a)
        expected = pyarrow.array(py_array + py_array, pyarrow.timestamp("us"))
        self.assertEqual(b, expected)
        del a
        del b
        del expected
        # No leak of C++ memory
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())

    def test_timestamp_tz_python(self):
        """
        Python -> Rust -> Python
        """
        old_allocated = pyarrow.total_allocated_bytes()
        py_array = [
            None,
            datetime(2021, 1, 1, 1, 1, 1, 1, tzinfo=timezone("America/New_York")),
            datetime(2020, 3, 9, 1, 1, 1, 1, tzinfo=timezone("America/New_York")),
        ]
        a = pyarrow.array(py_array, pyarrow.timestamp("us", tz="America/New_York"))
        b = arrow_pyarrow_integration_testing.concatenate(a)
        expected = pyarrow.array(
            py_array + py_array, pyarrow.timestamp("us", tz="America/New_York")
        )
        self.assertEqual(b, expected)
        del a
        del b
        del expected
        # No leak of C++ memory
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())

    def test_decimal_python(self):
        """
        Python -> Rust -> Python
        """
        old_allocated = pyarrow.total_allocated_bytes()
        py_array = [round(Decimal(123.45), 2), round(Decimal(-123.45), 2), None]
        a = pyarrow.array(py_array, pyarrow.decimal128(6, 2))
        b = arrow_pyarrow_integration_testing.round_trip(a)
        self.assertEqual(a, b)
        del a
        del b
        # No leak of C++ memory
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())

    def test_list_array(self):
        """
        Python -> Rust -> Python
        """
        old_allocated = pyarrow.total_allocated_bytes()
        a = pyarrow.array([[], None, [1, 2], [4, 5, 6]], pyarrow.list_(pyarrow.int64()))
        b = arrow_pyarrow_integration_testing.round_trip(a)

        b.validate(full=True)
        assert a.to_pylist() == b.to_pylist()
        assert a.type == b.type
        del a
        del b
        # No leak of C++ memory
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())
