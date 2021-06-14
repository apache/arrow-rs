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

import contextlib

import pytest
import pyarrow as pa

from arrow_pyarrow_integration_testing import PyDataType, PyField
import arrow_pyarrow_integration_testing as rust

from pytz import timezone


@contextlib.contextmanager
def no_pyarrow_leak():
    # No leak of C++ memory
    old_allocation = pa.total_allocated_bytes()
    try:
        yield
    finally:
        assert pa.total_allocated_bytes() == old_allocation


@pytest.fixture(autouse=True)
def assert_pyarrow_leak():
    # automatically applied to all test cases
    with no_pyarrow_leak():
        yield


_supported_pyarrow_types = [
    pa.null(),
    pa.bool_(),
    pa.int32(),
    pa.time32("s"),
    pa.time64("us"),
    pa.date32(),
    pa.float16(),
    pa.float32(),
    pa.float64(),
    pa.string(),
    pa.binary(),
    pa.large_string(),
    pa.large_binary(),
    pa.list_(pa.int32()),
    pa.large_list(pa.uint16()),
    pa.struct(
        [
            pa.field("a", pa.int32()),
            pa.field("b", pa.int8()),
            pa.field("c", pa.string()),
        ]
    ),
    pa.struct(
        [
            pa.field("a", pa.int32(), nullable=False),
            pa.field("b", pa.int8(), nullable=False),
            pa.field("c", pa.string()),
        ]
    ),
]

_unsupported_pyarrow_types = [
    pa.timestamp("us"),
    pa.timestamp("us", tz="UTC"),
    pa.timestamp("us", tz="Europe/Paris"),
    pa.duration("s"),
    pa.decimal128(19, 4),
    pa.decimal256(76, 38),
    pa.binary(10),
    pa.list_(pa.int32(), 2),
    pa.map_(pa.string(), pa.int32()),
    pa.union(
        [pa.field("a", pa.binary(10)), pa.field("b", pa.string())],
        mode=pa.lib.UnionMode_DENSE,
    ),
    pa.union(
        [pa.field("a", pa.binary(10)), pa.field("b", pa.string())],
        mode=pa.lib.UnionMode_DENSE,
        type_codes=[4, 8],
    ),
    pa.union(
        [pa.field("a", pa.binary(10)), pa.field("b", pa.string())],
        mode=pa.lib.UnionMode_SPARSE,
    ),
    pa.union(
        [
            pa.field("a", pa.binary(10), nullable=False),
            pa.field("b", pa.string()),
        ],
        mode=pa.lib.UnionMode_SPARSE,
    ),
]


@pytest.mark.parametrize("pyarrow_type", _supported_pyarrow_types, ids=str)
def test_type_roundtrip(pyarrow_type):
    ty = PyDataType.from_pyarrow(pyarrow_type)
    restored = ty.to_pyarrow()
    assert restored == pyarrow_type
    assert restored is not pyarrow_type


@pytest.mark.parametrize("pyarrow_type", _unsupported_pyarrow_types, ids=str)
def test_type_roundtrip_raises(pyarrow_type):
    with pytest.raises(Exception):
        PyDataType.from_pyarrow(pyarrow_type)


def test_dictionary_type_roundtrip():
    # the dictionary type conversion is incomplete
    pyarrow_type = pa.dictionary(pa.int32(), pa.string())
    ty = PyDataType.from_pyarrow(pyarrow_type)
    assert ty.to_pyarrow() == pa.int32()


# Missing implementation in pyarrow
# @pytest.mark.parametrize('pyarrow_type', _supported_pyarrow_types, ids=str)
# def test_field_roundtrip(pyarrow_type):
#     for nullable in [True, False]:
#         pyarrow_field = pa.field("test", pyarrow_type, nullable=nullable)
#         field = PyField.from_pyarrow(pyarrow_field)
#         assert field.to_pyarrow() == pyarrow_field


def test_primitive_python():
    """
    Python -> Rust -> Python
    """
    a = pa.array([1, 2, 3])
    b = rust.double(a)
    assert b == pa.array([2, 4, 6])
    del a
    del b


def test_primitive_rust():
    """
    Rust -> Python -> Rust
    """

    def double(array):
        array = array.to_pylist()
        return pa.array([x * 2 if x is not None else None for x in array])

    is_correct = rust.double_py(double)
    assert is_correct


def test_string_python():
    """
    Python -> Rust -> Python
    """
    a = pa.array(["a", None, "ccc"])
    b = rust.substring(a, 1)
    assert b == pa.array(["", None, "cc"])
    del a
    del b


def test_time32_python():
    """
    Python -> Rust -> Python
    """
    a = pa.array([None, 1, 2], pa.time32("s"))
    b = rust.concatenate(a)
    expected = pa.array([None, 1, 2] + [None, 1, 2], pa.time32("s"))
    assert b == expected
    del a
    del b
    del expected


def test_list_array():
    """
    Python -> Rust -> Python
    """
    a = pa.array([[], None, [1, 2], [4, 5, 6]], pa.list_(pa.int64()))
    b = rust.round_trip(a)
    b.validate(full=True)
    assert a.to_pylist() == b.to_pylist()
    assert a.type == b.type
    del a
    del b


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
