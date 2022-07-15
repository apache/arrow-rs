# Contributing

Thank you for thinking of contributing! We very much welcome contributions from the community. To make the process
easier and more valuable for everyone involved we have a few rules and guidelines to follow.

Anyone with a Github account is free to file issues on the project. However, if you want to contribute documentation or
code then you will need to sign InfluxData's Individual Contributor License Agreement (CLA), which can be found with
more information [on our website](https://www.influxdata.com/legal/cla/).

## Submitting Issues and Feature Requests

Before you file an [issue](https://github.com/influxdata/object_store_rs/issues/new), please search existing issues in
case
the same or similar issues have already been filed. If you find an existing open ticket covering your issue then please
avoid adding "👍" or "me too" comments; Github notifications can cause a lot of noise for the project maintainers who
triage the back-log.

However, if you have a new piece of information for an existing ticket and you think it may help the investigation or
resolution, then please do add it as a comment!

You can signal to the team that you're experiencing an existing issue with one of Github's emoji reactions (these are a
good way to add "weight" to an issue from a prioritization perspective).

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
$ podman run --rm -it -p 4566:4566 -p 4510-4559:4510-4559 localstack/localstack
```

Setup environment

```
export TEST_INTEGRATION=1
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_ENDPOINT=http://127.0.0.1:4566
export OBJECT_STORE_BUCKET=test-bucket
```

Create a bucket using the AWS CLI

```
podman run --net=host --env-host amazon/aws-cli --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket
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

```
$ cargo test --features azure
```

### GCP

We don't have a good story yet for testing the GCP integration locally. You will need to create a GCS bucket, a
service account that has access to it, and use this to run the tests.
