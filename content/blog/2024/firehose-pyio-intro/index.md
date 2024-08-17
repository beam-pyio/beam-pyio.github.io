---
title: Introduction to Firehose PyIO Connector
date: 2024-07-18
draft: false
featured: true
comment: false
toc: false
reward: false
pinned: false
carousel: false
featuredImage: false
series:
tags:
categories:
  - demonstration
tags: 
authors:
  - JaehyeonKim
images: []
# description: To be updated...
---

[Amazon Data Firehose](https://aws.amazon.com/firehose/) is a fully managed service for delivering real-time streaming data to destinations such as Amazon Simple Storage Service (Amazon S3), Amazon Redshift, Amazon OpenSearch Service and Amazon OpenSearch Serverless. The Apache Beam Python I/O connector for Amazon Data Firehose (`firehose_pyio`) provides a data sink feature that facilitates integration with those services.

<!--more-->

## Installation

The connector can be installed from PyPI.

```bash
pip install firehose_pyio
```

## Usage

### Sink Connector

It has the main composite transform ([`WriteToFirehose`](https://beam-pyio.github.io/firehose_pyio/autoapi/firehose_pyio/io/index.html#firehose_pyio.io.WriteToFirehose)), and it expects a list or tuple _PCollection_ element. If the element is a tuple, the tuple's first element is taken. If the element is not of the accepted types, you can apply the [`GroupIntoBatches`](https://beam.apache.org/documentation/transforms/python/aggregation/groupintobatches/) or [`BatchElements`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements) transform beforehand. Then, the element is sent into a Firehose delivery stream using the [`put_record_batch`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/firehose/client/put_record_batch.html) method of the boto3 package. Note that the above batch transforms can also be useful to overcome the API limitation listed below.

- Each `PutRecordBatch` request supports up to 500 records. Each record in the request can be as large as 1,000 KB (before base64 encoding), up to a limit of 4 MB for the entire request. These limits cannot be changed.

The transform also has options that control individual records and handle failed records.

- _jsonify_ - A flag that indicates whether to convert a record into JSON. Note that a record should be of _bytes_, _bytearray_ or file-like object, and, if it is not of a supported type (e.g. integer), we can convert it into a Json string by specifying this flag to _True_.
- _multiline_ - A flag that indicates whether to add a new line character (`\n`) to each record. It is useful to save records into a _CSV_ or _Jsonline_ file.
- _max_trials_ - The maximum number of trials when there is one or more failed records - it defaults to 3. Note that failed records after all trials are returned, which allows users to determine how to handle them subsequently.

#### Sink Connector Example

The example shows how to put records to a Firehose delivery stream that delivers data into an S3 bucket. We first need to create a delivery stream and related resources using the following Python script. The source can be found in the [**examples**](https://github.com/beam-pyio/firehose_pyio/tree/main/examples) folder of the connector repository.

```python
# examples/create_resources.py
import json
import boto3
from botocore.exceptions import ClientError

COMMON_NAME = "firehose-pyio-test"


def create_destination_bucket(bucket_name):
    client = boto3.client("s3")
    suffix = client._client_config.region_name
    client.create_bucket(Bucket=f"{bucket_name}-{suffix}")


def create_firehose_iam_role(role_name):
    assume_role_policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "firehose.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    client = boto3.client("iam")
    try:
        return client.get_role(RoleName=role_name)
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchEntity":
            resp = client.create_role(
                RoleName=role_name, AssumeRolePolicyDocument=assume_role_policy_document
            )
            client.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess",
            )
            return resp


def create_delivery_stream(delivery_stream_name, role_arn, bucket_name):
    client = boto3.client("firehose")
    suffix = client._client_config.region_name
    try:
        client.create_delivery_stream(
            DeliveryStreamName=delivery_stream_name,
            DeliveryStreamType="DirectPut",
            S3DestinationConfiguration={
                "RoleARN": role_arn,
                "BucketARN": f"arn:aws:s3:::{bucket_name}-{suffix}",
                "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 0},
            },
        )
    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceInUseException":
            pass
        else:
            raise error


if __name__ == "__main__":
    print("create a destination bucket...")
    create_destination_bucket(COMMON_NAME)
    print("create an iam role...")
    iam_resp = create_firehose_iam_role(COMMON_NAME)
    print("create a delivery stream...")
    create_delivery_stream(COMMON_NAME, iam_resp["Role"]["Arn"], COMMON_NAME)
```


The main example script is constructed so that it (1) deletes all existing objects in the S3 bucket, (2) runs the example pipeline and (3) prints contents of the object(s) created by the pipeline.

The pipeline begins with creating sample elements where each element is a dictionary that has the `id`, `name` and `created_at` (created date time) attributes. Then, we apply the following two transforms before we apply the main transform (`WriteToFirehose`).

- `DatetimeToStr` - It converts the `created_at` attribute values into string because the Python `datetime` class cannot be converted into Json by default.
- `BatchElements` - It batches the elements into the minimum batch size of 50. It prevents the individual dictionary element from being pushed into the `WriteToFirehose` transform.

In the `WriteToFirehose` transform, it is configured that individual records are converted into JSON (`jsonify=True`) as well as a new line character is appended (`multiline=True`). The former is required because the Python dictionary is not a supported data type while the latter makes the records are saved as JSONLines.

```python
# examples/pipeline.py
import argparse
import datetime
import random
import string
import logging
import boto3
import time

import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from firehose_pyio.io import WriteToFirehose


def get_all_contents(bucket_name):
    client = boto3.client("s3")
    bucket_objects = client.list_objects_v2(
        Bucket=f"{bucket_name}-{client._client_config.region_name}"
    )
    return bucket_objects.get("Contents") or []


def delete_all_objects(bucket_name):
    client = boto3.client("s3")
    contents = get_all_contents(bucket_name)
    for content in contents:
        client.delete_object(
            Bucket=f"{bucket_name}-{client._client_config.region_name}",
            Key=content["Key"],
        )


def print_bucket_contents(bucket_name):
    client = boto3.client("s3")
    contents = get_all_contents(bucket_name)
    for content in contents:
        resp = client.get_object(
            Bucket=f"{bucket_name}-{client._client_config.region_name}",
            Key=content["Key"],
        )
        print(f"Key - {content['Key']}")
        print(resp["Body"].read().decode())


def create_records(n=100):
    return [
        {
            "id": i,
            "name": "".join(random.choices(string.ascii_letters, k=5)).lower(),
            "created_at": datetime.datetime.now(),
        }
        for i in range(n)
    ]


def convert_ts(record: dict):
    record["created_at"] = record["created_at"].isoformat(timespec="milliseconds")
    return record


def mask_secrets(d: dict):
    return {k: (v if k.find("aws") < 0 else "x" * len(v)) for k, v in d.items()}


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--stream_name",
        default="firehose-pyio-test",
        type=str,
        help="Delivery stream name",
    )
    parser.add_argument(
        "--num_records", default="100", type=int, help="Number of records"
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(f"known_args - {known_args}")
    print(f"pipeline options - {mask_secrets(pipeline_options.display_data())}")

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "CreateElements" >> beam.Create(create_records(known_args.num_records))
            | "DatetimeToStr" >> beam.Map(convert_ts)
            | "BatchElements" >> BatchElements(min_batch_size=50)
            | "WriteToFirehose"
            >> WriteToFirehose(
                delivery_stream_name=known_args.stream_name,
                jsonify=True,
                multiline=True,
                max_trials=3,
            )
        )

        logging.getLogger().setLevel(logging.WARN)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    BUCKET_NAME = "firehose-pyio-test"
    print(">> delete existing objects...")
    delete_all_objects(BUCKET_NAME)
    print(">> start pipeline...")
    run()
    time.sleep(1)
    print(">> print bucket contents...")
    print_bucket_contents(BUCKET_NAME)
```

We can run the pipeline on any runner that supports the Python SDK. Below shows an example of running the example pipeline on the Apache Flink Runner. Note that AWS related values (e.g. `aws_access_key_id`) can be specified as pipeline arguments because the package has a dedicated pipeline option ([`FirehoseOptions`](https://github.com/beam-pyio/firehose_pyio/blob/main/src/firehose_pyio/options.py#L21)) that parses them. Once the pipeline runs successfully, the script continues to read the contents of the file object(s) that are created by the connector.

```bash
python examples/pipeline.py \
    --runner=FlinkRunner \
    --parallelism=1 \
    --aws_access_key_id=$AWS_ACCESS_KEY_ID \
    --aws_secret_access_key=$AWS_SECRET_ACCESS_KEY \
    --region_name=$AWS_DEFAULT_REGION
```

```bash
>> start pipeline...
known_args - Namespace(stream_name='firehose-pyio-test', num_records=100)
pipeline options - {'runner': 'FlinkRunner', 'save_main_session': True, 'parallelism': 1, 'aws_access_key_id': 'xxxxxxxxxxxxxxxxxxxx', 'aws_secret_access_key': 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 'region_name': 'us-east-1'}
>> print bucket contents...
Key - 2024/07/21/01/firehose-pyio-test-1-2024-07-21-01-49-51-5fd0fc8d-b656-4f7b-9bc6-5039975d941f
{"id": 50, "name": "amlis", "created_at": "2024-07-21T11:49:32.536"}
{"id": 51, "name": "bqvhr", "created_at": "2024-07-21T11:49:32.536"}
{"id": 52, "name": "stsbv", "created_at": "2024-07-21T11:49:32.536"}
{"id": 53, "name": "rttjg", "created_at": "2024-07-21T11:49:32.536"}
{"id": 54, "name": "avozb", "created_at": "2024-07-21T11:49:32.536"}
{"id": 55, "name": "fyesu", "created_at": "2024-07-21T11:49:32.536"}
{"id": 56, "name": "pvxlw", "created_at": "2024-07-21T11:49:32.536"}
{"id": 57, "name": "qyjlo", "created_at": "2024-07-21T11:49:32.536"}
{"id": 58, "name": "smhns", "created_at": "2024-07-21T11:49:32.536"}
...
```

Note that the following warning messages are printed if it installs the grpcio (1.65.x) package (see this [GitHub issue](https://github.com/grpc/grpc/issues/37178)). You can downgrade the package version to avoid those messages (eg `pip install grpcio==1.64.1`).

```bash
WARNING: All log messages before absl::InitializeLog() is called are written to STDERR
I0000 00:00:1721526572.886302   78332 config.cc:230] gRPC experiments enabled: call_status_override_on_cancellation, event_engine_dns, event_engine_listener, http2_stats_fix, monitoring_experiment, pick_first_new, trace_record_callops, work_serializer_clears_time_cache
I0000 00:00:1721526573.143589   78363 subchannel.cc:806] subchannel 0x7f4010001890 {address=ipv6:%5B::1%5D:58713, args={grpc.client_channel_factory=0x2ae79b0, grpc.default_authority=localhost:58713, grpc.internal.channel_credentials=0x297dda0, grpc.internal.client_channel_call_destination=0x7f407f3b23d0, grpc.internal.event_engine=0x7f4010013870, grpc.internal.security_connector=0x7f40100140e0, grpc.internal.subchannel_pool=0x21d3d00, grpc.max_receive_message_length=-1, grpc.max_send_message_length=-1, grpc.primary_user_agent=grpc-python/1.65.1, grpc.resource_quota=0x2a99310, grpc.server_uri=dns:///localhost:58713}}: connect failed (UNKNOWN:Failed to connect to remote host: connect: Connection refused (111) {created_time:"2024-07-21T11:49:33.143192444+10:00"}), backing off for 1000 ms
```

#### More Sink Connector Examples

More usage examples can be found in the [unit testing cases](https://github.com/beam-pyio/firehose_pyio/blob/main/tests/io_test.py). Some of them are covered here.

1. Only the list or tuple types are supported *PCollection* elements. In the following example, individual string elements are applied in the `WriteToFirehose`, and it raises the `TypeError`.

```python
def test_write_to_firehose_with_unsupported_types(self):
    # only the list type is supported!
    with self.assertRaises(TypeError):
        with TestPipeline(options=self.pipeline_opts) as p:
            (
                p
                | beam.Create(["one", "two", "three", "four"])
                | WriteToFirehose(self.delivery_stream_name, True, False)
            )
```

2. Jsonify the element if it is not of the bytes, bytearray or file-like object. In this example, the second element is a list of integers, and it should be converted into JSON (`jsonify=True`). Or we can convert it into string manually.

```python
def test_write_to_firehose_with_list_elements(self):
    with TestPipeline(options=self.pipeline_opts) as p:
        output = (
            p
            | beam.Create([["one", "two", "three", "four"], [1, 2, 3, 4]])
            | WriteToFirehose(self.delivery_stream_name, True, False)
        )
        assert_that(output, equal_to([]))

    bucket_contents = collect_bucket_contents(self.s3_client, self.bucket_name)
    self.assertSetEqual(
        set(bucket_contents), set(['"one""two""three""four"', "1234"])
    )
```

3. If an element is a tuple, its first element is applied to the `WriteToFirehose` transform.

```python
def test_write_to_firehose_with_tuple_elements(self):
    with TestPipeline(options=self.pipeline_opts) as p:
        output = (
            p
            | beam.Create([(1, ["one", "two", "three", "four"]), (2, [1, 2, 3, 4])])
            | WriteToFirehose(self.delivery_stream_name, True, False)
        )
        assert_that(output, equal_to([]))

    bucket_contents = collect_bucket_contents(self.s3_client, self.bucket_name)
    self.assertSetEqual(
        set(bucket_contents), set(['"one""two""three""four"', "1234"])
    )
```

4. We can batch an element if it is not of the supported types. Note that, a new line character (`\n`) is appended to each record, and it is particularly useful for saving a CSV or JSONLines file to S3.

```python
def test_write_to_firehose_with_list_multilining(self):
    with TestPipeline(options=self.pipeline_opts) as p:
        output = (
            p
            | beam.Create(["one", "two", "three", "four"])
            | BatchElements(min_batch_size=2, max_batch_size=2)
            | WriteToFirehose(self.delivery_stream_name, False, True)
        )
        assert_that(output, equal_to([]))

    bucket_contents = collect_bucket_contents(self.s3_client, self.bucket_name)
    self.assertSetEqual(set(bucket_contents), set(["one\ntwo\n", "three\nfour\n"]))

def test_write_to_firehose_with_tuple_multilining(self):
    with TestPipeline(options=self.pipeline_opts) as p:
        output = (
            p
            | beam.Create([(1, "one"), (2, "three"), (1, "two"), (2, "four")])
            | GroupIntoBatches(batch_size=2)
            | WriteToFirehose(self.delivery_stream_name, False, True)
        )
        assert_that(output, equal_to([]))

    bucket_contents = collect_bucket_contents(self.s3_client, self.bucket_name)
    self.assertSetEqual(set(bucket_contents), set(["one\ntwo\n", "three\nfour\n"]))
```

5. Failed records after all trials are returned. The following example configures only a single record is saved successfully into a delivery stream in each trial for 3 times. After all trials, the failed record (`four`) is returned from the `WriteToFirehose` transform. It is up to the user how to handle failed records.

```python
def test_write_to_firehose_retry_with_failed_elements(self):
    with TestPipeline() as p:
        output = (
            p
            | beam.Create(["one", "two", "three", "four"])
            | BatchElements(min_batch_size=4)
            | WriteToFirehose(
                "non-existing-delivery-stream", False, False, 3, {"num_success": 1}
            )
        )
        assert_that(output, equal_to(["four"]))
```