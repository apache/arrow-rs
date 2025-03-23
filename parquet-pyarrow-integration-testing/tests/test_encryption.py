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

import base64
from pathlib import Path
import pytest

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

import parquet_pyarrow_integration_testing as rust


@pytest.mark.parametrize("double_wrapping", [False, True])
def test_write_rust_read_python(tmp_path: Path, double_wrapping: bool):
    file_path = tmp_path / "test.parquet"
    rust.write_encrypted_parquet(file_path.as_posix(), double_wrapping)

    crypto_factory = pe.CryptoFactory(kms_client_factory)

    kms_connection_config = pe.KmsConnectionConfig()
    decryption_config = pe.DecryptionConfiguration()
    decryption_properties = crypto_factory.file_decryption_properties(
            kms_connection_config, decryption_config)

    parquet_file = pq.ParquetFile(
            file_path, decryption_properties=decryption_properties)
    table = parquet_file.read()

    assert len(table) > 0


@pytest.mark.parametrize("double_wrapping", [False, True])
def test_write_python_read_rust(tmp_path: Path, double_wrapping: bool):
    file_path = tmp_path / "test.parquet"

    crypto_factory = pe.CryptoFactory(kms_client_factory)

    kms_connection_config = pe.KmsConnectionConfig()

    encryption_config = pe.EncryptionConfiguration(
        footer_key="kf",
        column_keys={
            "kc1": ["x"],
            "kc2": ["y"],
        },
        double_wrapping=double_wrapping)
    encryption_properties = crypto_factory.file_encryption_properties(
            kms_connection_config, encryption_config)

    num_rows = 10
    table = pa.Table.from_pydict({
        "id": pa.array([i for i in range(num_rows)], type=pa.int32()),
        "x": pa.array([i / 10.0 for i in range(num_rows)], type=pa.float32()),
        "y": pa.array([i / 100.0 for i in range(num_rows)], type=pa.float32()),
    })

    with pq.ParquetWriter(file_path, table.schema,
                         encryption_properties=encryption_properties) as writer:
           writer.write_table(table)

    rust.read_encrypted_parquet(file_path.as_posix())


class _TestKmsClient(pe.KmsClient):
    def __init__(self, _kms_connection_configuration):
      pe.KmsClient.__init__(self)
      self._keys = {
          'kf': b'0123456789012345',
          'kc1': b'1234567890123450',
          'kc2': b'1234567890123451',
      }

    def wrap_key(self, key_bytes, master_key_identifier):
        key = self._keys[master_key_identifier]
        aad = master_key_identifier.encode('utf-8')
        nonce = get_random_bytes(12)
        cipher = AES.new(key, AES.MODE_GCM, nonce=nonce, mac_len=16)
        cipher.update(aad)
        encrypted, tag = cipher.encrypt_and_digest(key_bytes)
        wrapped_key_bytes = bytes(cipher.nonce) + encrypted + tag
        return base64.b64encode(wrapped_key_bytes).decode('utf-8')

    def unwrap_key(self, wrapped_key, master_key_identifier):
        key = self._keys[master_key_identifier]
        aad = master_key_identifier.encode('utf-8')
        wrapped_key_bytes = base64.b64decode(wrapped_key)
        tag = wrapped_key_bytes[-16:]
        nonce = wrapped_key_bytes[:12]
        encrypted = wrapped_key_bytes[12:-16]
        assert len(encrypted) == 16
        cipher = AES.new(key, AES.MODE_GCM, nonce=nonce, mac_len=16)
        cipher.update(aad)
        return cipher.decrypt_and_verify(encrypted, tag)


def kms_client_factory(kms_connection_configuration):
   return _TestKmsClient(kms_connection_configuration)
