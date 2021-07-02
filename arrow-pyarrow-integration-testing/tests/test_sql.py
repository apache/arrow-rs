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
import datetime
import decimal
import string

import pytest
import pyarrow as pa
import pytz

from arrow_pyarrow_integration_testing import PyDataType, PyField, PySchema
import arrow_pyarrow_integration_testing as rust


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
    pa.timestamp("us"),
    pa.timestamp("us", tz="UTC"),
    pa.timestamp("us", tz="Europe/Paris"),
    pa.float16(),
    pa.float32(),
    pa.float64(),
    pa.decimal128(19, 4),
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
    pa.decimal256(76, 38),
    pa.duration("s"),
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


@pytest.mark.parametrize('pyarrow_type', _supported_pyarrow_types, ids=str)
def test_field_roundtrip(pyarrow_type):
    pyarrow_field = pa.field("test", pyarrow_type, nullable=True)
    field = PyField.from_pyarrow(pyarrow_field)
    assert field.to_pyarrow() == pyarrow_field

    if pyarrow_type != pa.null():
        # A null type field may not be non-nullable
        pyarrow_field = pa.field("test", pyarrow_type, nullable=False)
        field = PyField.from_pyarrow(pyarrow_field)
        assert field.to_pyarrow() == pyarrow_field


def test_schema_roundtrip():
    pyarrow_fields = zip(string.ascii_lowercase, _supported_pyarrow_types)
    pyarrow_schema = pa.schema(pyarrow_fields)
    schema = PySchema.from_pyarrow(pyarrow_schema)
    assert schema.to_pyarrow() == pyarrow_schema


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


def test_timestamp_python():
    """
    Python -> Rust -> Python
    """
    data = [
        None,
        datetime.datetime(2021, 1, 1, 1, 1, 1, 1),
        datetime.datetime(2020, 3, 9, 1, 1, 1, 1),
    ]
    a = pa.array(data, pa.timestamp("us"))
    b = rust.concatenate(a)
    expected = pa.array(data + data, pa.timestamp("us"))
    assert b == expected
    del a
    del b
    del expected


def test_timestamp_tz_python():
    """
    Python -> Rust -> Python
    """
    tzinfo = pytz.timezone("America/New_York")
    pyarrow_type = pa.timestamp("us", tz="America/New_York")
    data = [
        None,
        datetime.datetime(2021, 1, 1, 1, 1, 1, 1, tzinfo=tzinfo),
        datetime.datetime(2020, 3, 9, 1, 1, 1, 1, tzinfo=tzinfo),
    ]
    a = pa.array(data, type=pyarrow_type)
    b = rust.concatenate(a)
    expected = pa.array(data * 2, type=pyarrow_type)
    assert b == expected
    del a
    del b
    del expected


def test_decimal_python():
    """
    Python -> Rust -> Python
    """
    data = [
        round(decimal.Decimal(123.45), 2),
        round(decimal.Decimal(-123.45), 2),
        None
    ]
    a = pa.array(data, pa.decimal128(6, 2))
    b = rust.round_trip(a)
    assert a == b
    del a
    del b
