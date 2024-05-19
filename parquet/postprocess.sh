#!/usr/bin/env bash

set -e

echo "Removing TProcessor"

sed -i "/use thrift::server::TProcessor;/d" $1

echo "Replacing TSerializable"

sed -i "s/impl TSerializable for/impl<'de> crate::thrift::TSerializable<'de> for/g" $1

echo "Rewriting write_to_out_protocol"

sed -i "s/fn write_to_out_protocol(&self, o_prot: &mut dyn TOutputProtocol)/fn write_to_out_protocol<T: TOutputProtocol>(\&self, o_prot: \&mut T)/g" $1

echo "Rewriting read_from_in_protocol"

sed -i "s/fn read_from_in_protocol(i_prot: &mut dyn TInputProtocol)/fn read_from_in_protocol<T: TInputProtocol>(i_prot: \&mut T)/g" $1

echo "Rewriting return value expectations"

sed -i "s/Ok(ret.expect(\"return value should have been constructed\"))/ret.ok_or_else(|| thrift::Error::Protocol(ProtocolError::new(ProtocolErrorKind::InvalidData, \"return value should have been constructed\")))/g" $1

echo "Rewriting TInputProtocol"

sed -i "s/T: TInputProtocol/T: crate::thrift::TInputProtocolRef<'de>/g" $1

echo "Rewriting read_string()"

sed -i "s/read_string()/read_str()/g" $1
sed -i "s/<String>/<std::borrow::Cow<'de, str>>/g" $1
sed -i "s/: String/: std::borrow::Cow<'de, str>/g" $1

echo "Rewriting read_bytes()"

sed -i "s/read_bytes()/read_buf()/g" $1
sed -i "s/<Vec<u8>>/<std::borrow::Cow<'de, [u8]>>/g" $1
sed -i "s/: Vec<u8>/: std::borrow::Cow<'de, [u8]>/g" $1



for d in ColumnChunk Statistics DataPageHeader DataPageHeaderV2 ColumnMetaData ColumnIndex FileMetaData FileCryptoMetaData AesGcmV1 EncryptionWithColumnKey PageHeader RowGroup AesGcmCtrV1 EncryptionAlgorithm KeyValue ColumnCryptoMetaData SchemaElement; do
  echo "Rewriting $d with lifetime"

  sed -i "s/enum $d {/enum $d<'de> {/g" $1
  sed -i "s/struct $d {/struct $d<'de> {/g" $1
  sed -i "s/for $d {/for $d<'de> {/g" $1
  sed -i "s/impl $d {/impl<'de> $d<'de> {/g" $1
  sed -i "s/<$d>/<$d<'de>>/g" $1
  sed -i "s/: $d/: $d<'de>/g" $1
  sed -i "s/($d)/($d<'de>)/g" $1
  sed -i "s/-> $d /-> $d<'de> /g" $1
done
