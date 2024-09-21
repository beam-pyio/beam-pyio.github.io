---
title: Introduction to DynamoDB PyIO Sink Connector
date: 2024-09-23
draft: true
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

[Amazon DynamoDB](https://aws.amazon.com/dynamodb/) is a serverless, NoSQL database service that allows you to develop modern applications at any scale. The [Apache Beam Python I/O connector for Amazon DynamoDB](https://github.com/beam-pyio/dynamodb_pyio) (`dynamodb_pyio`) aims to integrate with the database service by supporting source and sink connectors. Currently, the sink connector is available.

<!--more-->

## Installation

The connector can be installed from PyPI.

```bash
pip install dynamodb_pyio
```

## Usage

### Sink Connector

It has the main composite transform ([`WriteToDynamoDB`](https://beam-pyio.github.io/dynamodb_pyio/autoapi/dynamodb_pyio/io/index.html#dynamodb_pyio.io.WriteToDynamoDB)), and it expects a list or tuple _PCollection_ element. If the element is a tuple, the tuple's first element is taken. If the element is not of the accepted types, you can apply the [`GroupIntoBatches`](https://beam.apache.org/documentation/transforms/python/aggregation/groupintobatches/) or [`BatchElements`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements) transform beforehand. Then, the records of the element are written to a DynamoDB table with help of the [`batch_writer`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/batch_writer.html) of the boto3 package. Note that the batch writer will automatically handle buffering and sending items in batches. In addition, it will also automatically handle any unprocessed items and resend them as needed.

The transform also has an option that handles duplicate records - see the example below for more details.

- *dedup_pkeys* - List of keys to be used for deduplicating items in buffer.

#### Sink Connector Example

The example shows how to write records in batch to a DynamoDB table using the sink connector. The source can be found in the [**examples**](https://github.com/beam-pyio/dynamodb_pyio/tree/main/examples) folder of the connector repository.

The pipeline begins with creating a configurable number records (default 500) where each record has two attributes (`pk` and `sk`). The attribute values are configured so that the first half of the records have incremental values while the remaining half have a single value of 1. Therefore, as the attributes are the hash and sort key of the table, we can expect only 250 records in the DynamoDB table if we configure 500 records. Moreover, in spite of duplicate values, we do not encounter an error if we specify the `dedup_pkeys` value correctly.

After creating elements, we apply the `BatchElements` transform where the minimum and maximum batch sizes are set to 100 and 200 respectively. Note that it prevents individual dictionary elements from being pushed into the `WriteToDynamoDB` transform. Finally, batches of elements are written to the DynamoDB table using the `WriteToDynamoDB` transform.

```python
# pipeline.py
import argparse
import decimal
import logging

import boto3
from boto3.dynamodb.types import TypeDeserializer

import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from dynamodb_pyio.io import WriteToDynamoDB

TABLE_NAME = "dynamodb-pyio-test"


def get_table(table_name):
    resource = boto3.resource("dynamodb")
    return resource.Table(table_name)


def create_table(table_name):
    client = boto3.client("dynamodb")
    try:
        client.describe_table(TableName=table_name)
        table_exists = True
    except Exception:
        table_exists = False
    if not table_exists:
        print(">> create table...")
        params = {
            "TableName": table_name,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "N"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
        client.create_table(**params)
        get_table(table_name).wait_until_exists()


def to_int_if_decimal(v):
    try:
        if isinstance(v, decimal.Decimal):
            return int(v)
        else:
            return v
    except Exception:
        return v


def scan_table(**kwargs):
    client = boto3.client("dynamodb")
    paginator = client.get_paginator("scan")
    page_iterator = paginator.paginate(**kwargs)
    items = []
    for page in page_iterator:
        for document in page["Items"]:
            items.append(
                {
                    k: to_int_if_decimal(TypeDeserializer().deserialize(v))
                    for k, v in document.items()
                }
            )
    return sorted(items, key=lambda d: d["sk"])


def truncate_table(table_name):
    records = scan_table(TableName=TABLE_NAME)
    table = get_table(table_name)
    with table.batch_writer() as batch:
        for record in records:
            batch.delete_item(Key=record)


def mask_secrets(d: dict):
    return {k: (v if k.find("aws") < 0 else "x" * len(v)) for k, v in d.items()}


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--table_name", default=TABLE_NAME, type=str, help="DynamoDB table name"
    )
    parser.add_argument(
        "--num_records", default="500", type=int, help="Number of records"
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(f"known_args - {known_args}")
    print(f"pipeline options - {mask_secrets(pipeline_options.display_data())}")

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "CreateElements"
            >> beam.Create(
                [
                    {
                        "pk": str(int(1 if i >= known_args.num_records / 2 else i)),
                        "sk": int(1 if i >= known_args.num_records / 2 else i),
                    }
                    for i in range(known_args.num_records)
                ]
            )
            | "BatchElements" >> BatchElements(min_batch_size=100, max_batch_size=200)
            | "WriteToDynamoDB"
            >> WriteToDynamoDB(
                table_name=known_args.table_name, dedup_pkeys=["pk", "sk"]
            )
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    create_table(TABLE_NAME)
    print(">> start pipeline...")
    run()
    print(">> check number of records...")
    print(len(scan_table(TableName=TABLE_NAME)))
    print(">> truncate table...")
    truncate_table(TABLE_NAME)
```

We can run the pipeline on any runner that supports the Python SDK. Below shows an example of running the example pipeline on the Apache Flink Runner. Note that AWS related values (e.g. `aws_access_key_id`) can be specified as pipeline arguments because the package has a dedicated pipeline option ([`DynamoDBOptions`](https://beam-pyio.github.io/dynamodb_pyio/autoapi/dynamodb_pyio/options/)) that parses them. Once the pipeline runs successfully, the script checks the number of records that are created by the connector followed by truncating the table.

```bash
python examples/pipeline.py \
    --runner=FlinkRunner \
    --parallelism=1 \
    --aws_access_key_id=$AWS_ACCESS_KEY_ID \
    --aws_secret_access_key=$AWS_SECRET_ACCESS_KEY \
    --region_name=$AWS_DEFAULT_REGION
```

```bash
>> create table...
>> start pipeline...
known_args - Namespace(table_name='dynamodb-pyio-test', num_records=500)
pipeline options - {'runner': 'FlinkRunner', 'save_main_session': True, 'parallelism': 1, 'aws_access_key_id': 'xxxxxxxxxxxxxxxxxxxx', 'aws_secret_access_key': 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 'region_name': 'us-east-1'}
...
INFO:root:total 100 elements processed...
INFO:root:total 100 elements processed...
INFO:root:total 200 elements processed...
INFO:root:total 100 elements processed...
INFO:root:BatchElements statistics: element_count=500 batch_count=4 next_batch_size=101 timings=[(100, 0.9067182540893555), (200, 1.815131664276123), (100, 0.22190475463867188)]
...
>> check number of records...
...
250
>> truncate table...
```

Note that the following warning messages are printed if it installs the grpcio (1.65.x) package (see this [GitHub issue](https://github.com/grpc/grpc/issues/37178)). You can downgrade the package version to avoid those messages (e.g. `pip install grpcio==1.64.1`).

```bash
WARNING: All log messages before absl::InitializeLog() is called are written to STDERR
I0000 00:00:1721526572.886302   78332 config.cc:230] gRPC experiments enabled: call_status_override_on_cancellation, event_engine_dns, event_engine_listener, http2_stats_fix, monitoring_experiment, pick_first_new, trace_record_callops, work_serializer_clears_time_cache
I0000 00:00:1721526573.143589   78363 subchannel.cc:806] subchannel 0x7f4010001890 {address=ipv6:%5B::1%5D:58713, args={grpc.client_channel_factory=0x2ae79b0, grpc.default_authority=localhost:58713, grpc.internal.channel_credentials=0x297dda0, grpc.internal.client_channel_call_destination=0x7f407f3b23d0, grpc.internal.event_engine=0x7f4010013870, grpc.internal.security_connector=0x7f40100140e0, grpc.internal.subchannel_pool=0x21d3d00, grpc.max_receive_message_length=-1, grpc.max_send_message_length=-1, grpc.primary_user_agent=grpc-python/1.65.1, grpc.resource_quota=0x2a99310, grpc.server_uri=dns:///localhost:58713}}: connect failed (UNKNOWN:Failed to connect to remote host: connect: Connection refused (111) {created_time:"2024-07-21T11:49:33.143192444+10:00"}), backing off for 1000 ms
```

#### More Sink Connector Examples

More usage examples can be found in the [unit testing cases](https://github.com/beam-pyio/dynamodb_pyio/blob/main/tests/io_test.py). Some of them are covered here.

1. The transform can process many records, thanks to the batch writer.

```python
def test_write_to_dynamodb_with_large_items(self):
    # batch writer automatically handles buffering and sending items in batches
    records = [{"pk": str(i), "sk": i} for i in range(5000)]
    with TestPipeline(options=self.pipeline_opts) as p:
        (p | beam.Create([records]) | WriteToDynamoDB(table_name=self.table_name))
    self.assertListEqual(records, scan_table(TableName=self.table_name))
```

2. Only the list or tuple types are supported *PCollection* elements. In the following example, individual dictionary elements are applied in the `WriteToDynamoDB`, and it raises the `DynamoDBClientError`.

```python
def test_write_to_dynamodb_with_unsupported_record_type(self):
    # supported types are list or tuple where the second element is a list!
    records = [{"pk": str(i), "sk": i} for i in range(20)]
    with self.assertRaises(DynamoDBClientError):
        with TestPipeline(options=self.pipeline_opts) as p:
            (p | beam.Create(records) | WriteToDynamoDB(table_name=self.table_name))
```

3. Incorrect key attribute data types throws an error.

```python
def test_write_to_dynamodb_with_wrong_data_type(self):
    # pk and sk should be string and number respectively
    records = [{"pk": i, "sk": str(i)} for i in range(20)]
    with self.assertRaises(DynamoDBClientError):
        with TestPipeline(options=self.pipeline_opts) as p:
            (
                p
                | beam.Create([records])
                | WriteToDynamoDB(table_name=self.table_name)
            )
```

4. Duplicate records are not allowed unless the `dedup_pkeys` value is correctly specified.

```python
def test_write_to_dynamodb_duplicate_records_without_dedup_keys(self):
    records = [{"pk": str(1), "sk": 1} for _ in range(20)]
    with self.assertRaises(DynamoDBClientError):
        with TestPipeline(options=self.pipeline_opts) as p:
            (
                p
                | beam.Create([records])
                | WriteToDynamoDB(table_name=self.table_name)
            )

def test_write_to_dynamodb_duplicate_records_with_dedup_keys(self):
    records = [{"pk": str(1), "sk": 1} for _ in range(20)]
    with TestPipeline(options=self.pipeline_opts) as p:
        (
            p
            | beam.Create([records])
            | WriteToDynamoDB(table_name=self.table_name, dedup_pkeys=["pk", "sk"])
        )
    self.assertListEqual(records[:1], scan_table(TableName=self.table_name))
```

4. We can control batches of elements further with the `BatchElements` or `GroupIntoBatches` transform.

```python
def test_write_to_dynamodb_with_batch_elements(self):
    records = [{"pk": str(i), "sk": i} for i in range(20)]
    with TestPipeline(options=self.pipeline_opts) as p:
        (
            p
            | beam.Create(records)
            | BatchElements(min_batch_size=10, max_batch_size=10)
            | WriteToDynamoDB(table_name=self.table_name)
        )
    self.assertListEqual(records, scan_table(TableName=self.table_name))

def test_write_to_dynamodb_with_group_into_batches(self):
    records = [(i % 2, {"pk": str(i), "sk": i}) for i in range(20)]
    with TestPipeline(options=self.pipeline_opts) as p:
        (
            p
            | beam.Create(records)
            | GroupIntoBatches(batch_size=10)
            | WriteToDynamoDB(table_name=self.table_name)
        )
    self.assertListEqual(
        [r[1] for r in records], scan_table(TableName=self.table_name)
    )
```