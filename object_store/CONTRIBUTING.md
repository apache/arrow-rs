<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Development instructions

## Running Tests

Tests can be run using `cargo`

```shell
cargo test
```

## Running Integration Tests

By default, integration tests are not run. To run them you will need to set `TEST_INTEGRATION=1` and then provide the
necessary configuration for that object store

### AWS

To test the S3 integration against [localstack](https://localstack.cloud/)

First start up a container running localstack

```
$ podman run -d -p 4566:4566 localstack/localstack:2.0
$ podman run -d -p 1338:1338 amazon/amazon-ec2-metadata-mock:v1.9.2 --imdsv2
```

Setup environment

```
export TEST_INTEGRATION=1
export OBJECT_STORE_AWS_DEFAULT_REGION=us-east-1
export OBJECT_STORE_AWS_ACCESS_KEY_ID=test
export OBJECT_STORE_AWS_SECRET_ACCESS_KEY=test
export OBJECT_STORE_AWS_ENDPOINT=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export OBJECT_STORE_BUCKET=test-bucket
```

Create a bucket using the AWS CLI

```
podman run --net=host --env-host amazon/aws-cli --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket
```

Or directly with:

```
aws s3 mb s3://test-bucket --endpoint-url=http://localhost:4566
```

Run tests

```
$ cargo test --features aws
```

### Azure

To test the Azure integration
against [azurite](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio)

Startup azurite

```
$ podman run -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite
```

Create a bucket

```
$ podman run --net=host mcr.microsoft.com/azure-cli az storage container create -n test-bucket --connection-string 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;'
```

Run tests

```shell
AZURE_USE_EMULATOR=1 \
TEST_INTEGRATION=1 \
OBJECT_STORE_BUCKET=test-bucket \
AZURE_STORAGE_ACCOUNT=devstoreaccount1 \
AZURE_STORAGE_ACCESS_KEY=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw== \
cargo test --features azure
```

### GCP

To test the GCS integration, we use [Fake GCS Server](https://github.com/fsouza/fake-gcs-server)

Startup the fake server:

```shell
docker run -p 4443:4443 tustvold/fake-gcs-server -scheme http
```

Configure the account:
```shell
curl -v -X POST --data-binary '{"name":"test-bucket"}' -H "Content-Type: application/json" "http://localhost:4443/storage/v1/b"
echo '{"gcs_base_url": "http://localhost:4443", "disable_oauth": true, "client_email": "", "private_key": ""}' > /tmp/gcs.json
```

Now run the tests:
```shell
TEST_INTEGRATION=1 \
OBJECT_STORE_BUCKET=test-bucket \
GOOGLE_SERVICE_ACCOUNT=/tmp/gcs.json \
cargo test -p object_store --features=gcp
```
