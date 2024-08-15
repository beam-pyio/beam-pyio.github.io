---
title: Introduction to SQS PyIO Connector
date: 2024-08-22
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

[Amazon Simple Queue Service (Amazon SQS)](https://aws.amazon.com/sqs/) offers a secure, durable, and available hosted queue that lets you integrate and decouple distributed software systems and components. The Apache Beam Python I/O connector for Amazon SQS (`sqs_pyio`) aims to integrate with the queue service by supporting a source and sinke connectors. Currently a sink connector is available.

<!--more-->

## Installation

The connector can be installed from PyPI.

```bash
pip install sqs_pyio
```

## Usage

### Sink Connector

It has the main composite transform ([`WriteToSqs`](https://beam-pyio.github.io/sqs_pyio/autoapi/sqs_pyio/io/index.html#sqs_pyio.io.WriteToSqs)), and it expects a list or tuple _PCollection_ element. If the element is a tuple, the tuple's first element is taken. If the element is not of the accepted types, you can apply the [`GroupIntoBatches`](https://beam.apache.org/documentation/transforms/python/aggregation/groupintobatches/) or [`BatchElements`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements) transform beforehand. Then, the element is sent into a SQS queue using the [`send_message_batch`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/send_message_batch.html) method of the boto3 package. Note that the above batch transforms can also be useful to overcome the API limitation listed below.

- Each `SendMessageBatch` request supports up to 10 messages. The maximum allowed individual message size and the maximum total payload size (the sum of the individual lengths of all of the batched messages) are both 256 KiB (262,144 bytes).

The transform also has options that handle failed records as listed below.

- *max_trials* - The maximum number of trials when there is one or more failed records - it defaults to 3. Note that failed records after all trials are returned by a tagged output, which allows users to determine how to handle them subsequently.
- *append_error* - Whether to append error details to failed records. Defaults to True.

As mentioned earlier, failed elements are returned by a tagged output where it is named as `write-to-sqs-failed-output` by default. You can change the name by specifying a different name using the `failed_output` argument.

#### Sink Connector Example

The example shows how to send messages in batch to a SQS queue using the sink connector and check the approximate number of messages in the queue. The source can be found in the [**examples**](https://github.com/beam-pyio/sqs_pyio/tree/main/examples) folder of the connector repository.

The pipeline begins with creating sample elements where each element is a dictionary that has the `Id` and `MessageBody` attributes. Then, we apply the `BatchElements` transform where the minimum and maximum batch sizes are set to 10. It prevents the individual dictionary element from being pushed into the `WriteToSqs` transform. Also it allows us to bypass the API limitation. Finally, in the `WriteToSqs` transform, it is configured that a maximum of three trials are made when there are failed elements (`max_trials=3`) and error details are appended to failed elements (`append_error=True`).

```python
import argparse
import time
import logging

import boto3
from botocore.exceptions import ClientError

import apache_beam as beam
from apache_beam.transforms.util import BatchElements
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from sqs_pyio.io import WriteToSqs

QUEUE_NAME = "sqs-pyio-test"


def split_into_chunks(max_num, size=10):
    lst = list(range(max_num))
    for i in range(0, max_num, size):
        yield lst[i : i + size]


def send_message_batch(queue_name, max_num):
    client = boto3.client("sqs")
    queue_url = get_queue_url(queue_name)
    chunks = split_into_chunks(max_num)
    for chunk in chunks:
        print(f"sending {len(chunk)} messages...")
        records = [{"Id": str(i), "MessageBody": str(i)} for i in chunk]
        client.send_message_batch(QueueUrl=queue_url, Entries=records)


def get_queue_url(queue_name):
    client = boto3.client("sqs")
    try:
        return client.get_queue_url(QueueName=queue_name)["QueueUrl"]
    except ClientError as error:
        if error.response["Error"]["QueryErrorCode"] == "QueueDoesNotExist":
            client.create_queue(QueueName=queue_name)
            return client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        else:
            raise error


def purge_queue(queue_name):
    client = boto3.client("sqs")
    queue_url = get_queue_url(queue_name)
    try:
        client.purge_queue(QueueUrl=queue_url)
    except ClientError as error:
        if error.response["Error"]["QueryErrorCode"] != "PurgeQueueInProgress":
            raise error


def check_number_of_messages(queue_name):
    client = boto3.client("sqs")
    queue_url = get_queue_url(queue_name)
    resp = client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    return f'{resp["Attributes"]["ApproximateNumberOfMessages"]} messages are found approximately!'


def mask_secrets(d: dict):
    return {k: (v if k.find("aws") < 0 else "x" * len(v)) for k, v in d.items()}


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--queue_name", default=QUEUE_NAME, type=str, help="SQS queue name"
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
            | "CreateElements"
            >> beam.Create(
                [
                    {"Id": str(i), "MessageBody": str(i)}
                    for i in range(known_args.num_records)
                ]
            )
            | "BatchElements" >> BatchElements(min_batch_size=10, max_batch_size=10)
            | "WriteToSqs"
            >> WriteToSqs(
                queue_name=known_args.queue_name,
                max_trials=3,
                append_error=True,
                failed_output="my-failed-output",
            )
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    # check if queue exists
    get_queue_url(QUEUE_NAME)
    print(">> start pipeline...")
    run()
    time.sleep(1)
    print(check_number_of_messages(QUEUE_NAME))
    print(">> purge existing messages...")
    purge_queue(QUEUE_NAME)
```

We can run the pipeline on any runner that supports the Python SDK. Below shows an example of running the example pipeline on the Apache Flink Runner. Note that AWS related values (e.g. `aws_access_key_id`) can be specified as pipeline arguments because the package has a dedicated pipeline option ([`SqsOptions`](https://github.com/beam-pyio/sqs_pyio/blob/main/src/sqs_pyio/options.py#L21)) that parses them. Once the pipeline runs successfully, the script checks the approximate number of messages that are created by the connector.

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
known_args - Namespace(queue_name='sqs-pyio-test', num_records=100)
pipeline options - {'runner': 'FlinkRunner', 'save_main_session': True, 'parallelism': 1, 'aws_access_key_id': 'xxxxxxxxxxxxxxxxxxxx', 'aws_secret_access_key': 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 'region_name': 'us-east-1'}
...
INFO:root:total 10, succeeded 10, failed 0...
INFO:root:total 10, succeeded 10, failed 0...
INFO:root:total 10, succeeded 10, failed 0...
INFO:root:total 10, succeeded 10, failed 0...
INFO:root:total 10, succeeded 10, failed 0...
INFO:root:total 10, succeeded 10, failed 0...
INFO:root:total 10, succeeded 10, failed 0...
INFO:root:total 10, succeeded 10, failed 0...
INFO:root:total 10, succeeded 10, failed 0...
INFO:root:total 10, succeeded 10, failed 0...
INFO:root:BatchElements statistics: element_count=100 batch_count=10 next_batch_size=10 timings=[(10, 0.44828295707702637), (10, 0.4457244873046875), (10, 0.4468190670013428), (10, 0.4548039436340332), (10, 0.4473147392272949), (10, 0.4668114185333252), (10, 0.4462318420410156), (10, 0.4522392749786377), (10, 0.44727039337158203)]
...
100 messages are found approximately!
>> purge existing messages...
```

Note that the following warning messages are printed if it installs the grpcio (1.65.x) package (see this [GitHub issue](https://github.com/grpc/grpc/issues/37178)). You can downgrad the package version to avoid those messages (eg `pip install grpcio==1.64.1`).

```bash
WARNING: All log messages before absl::InitializeLog() is called are written to STDERR
I0000 00:00:1721526572.886302   78332 config.cc:230] gRPC experiments enabled: call_status_override_on_cancellation, event_engine_dns, event_engine_listener, http2_stats_fix, monitoring_experiment, pick_first_new, trace_record_callops, work_serializer_clears_time_cache
I0000 00:00:1721526573.143589   78363 subchannel.cc:806] subchannel 0x7f4010001890 {address=ipv6:%5B::1%5D:58713, args={grpc.client_channel_factory=0x2ae79b0, grpc.default_authority=localhost:58713, grpc.internal.channel_credentials=0x297dda0, grpc.internal.client_channel_call_destination=0x7f407f3b23d0, grpc.internal.event_engine=0x7f4010013870, grpc.internal.security_connector=0x7f40100140e0, grpc.internal.subchannel_pool=0x21d3d00, grpc.max_receive_message_length=-1, grpc.max_send_message_length=-1, grpc.primary_user_agent=grpc-python/1.65.1, grpc.resource_quota=0x2a99310, grpc.server_uri=dns:///localhost:58713}}: connect failed (UNKNOWN:Failed to connect to remote host: connect: Connection refused (111) {created_time:"2024-07-21T11:49:33.143192444+10:00"}), backing off for 1000 ms
```

#### More Sink Connector Examples

More usage examples can be found in the [unit testing cases](https://github.com/beam-pyio/sqs_pyio/blob/main/tests/io_test.py). Some of them are covered here.

1. Only the list or tuple types are supported *PCollection* elements. In the following example, individual string elements are applied in the `WriteToFirehose`, and it raises the `SqsClientError`.

```python
def test_write_to_sqs_with_unsupported_record_type(self):
    # only the list type is supported!
    records = [{"Id": str(i), "MessageBody": str(i)} for i in range(3)]
    with self.assertRaises(SqsClientError):
        with TestPipeline(options=self.pipeline_opts) as p:
            (p | beam.Create(records) | WriteToSqs(queue_name=self.queue_name))
```

2. Both the list and tuple types are supported. Note the main output is tagged as *None* and we can check the elements by specifying the tag value (i.e. `output[None]`).

```python
def test_write_to_sqs_with_list_element(self):
    records = [{"Id": str(i), "MessageBody": str(i)} for i in range(3)]
    with TestPipeline(options=self.pipeline_opts) as p:
        output = p | beam.Create([records]) | WriteToSqs(queue_name=self.queue_name)
        assert_that(output[None], equal_to([]))

def test_write_to_sqs_with_tuple_element(self):
    records = [{"Id": str(i), "MessageBody": str(i)} for i in range(3)]
    with TestPipeline(options=self.pipeline_opts) as p:
        output = (
            p
            | beam.Create([("key", records)])
            | WriteToSqs(queue_name=self.queue_name)
        )
        assert_that(output[None], equal_to([]))
```

3. The `Id` and `MessageBody` attributes are mandatory and they should be the string type.

```python
def test_write_to_sqs_with_incorrect_message_data_type(self):
    # Id should be string
    records = [{"Id": i, "MessageBody": str(i)} for i in range(3)]
    with self.assertRaises(SqsClientError):
        with TestPipeline(options=self.pipeline_opts) as p:
            (p | beam.Create([records]) | WriteToSqs(queue_name=self.queue_name))

    # MessageBody should be string
    records = [{"Id": str(i), "MessageBody": i} for i in range(3)]
    with self.assertRaises(SqsClientError):
        with TestPipeline(options=self.pipeline_opts) as p:
            (p | beam.Create([records]) | WriteToSqs(queue_name=self.queue_name))
```

4. We can batch the elements with the `BatchElements` or `GroupIntoBatches` transform.

```python
def test_write_to_sqs_with_batch_elements(self):
    # BatchElements groups unkeyed elements into a list
    records = [{"Id": str(i), "MessageBody": str(i)} for i in range(3)]
    with TestPipeline(options=self.pipeline_opts) as p:
        output = (
            p
            | beam.Create(records)
            | BatchElements(min_batch_size=2, max_batch_size=2)
            | WriteToSqs(queue_name=self.queue_name)
        )
        assert_that(output[None], equal_to([]))

def test_write_to_sqs_with_group_into_batches(self):
    # GroupIntoBatches groups keyed elements into a list
    records = [(i % 2, {"Id": str(i), "MessageBody": str(i)}) for i in range(3)]
    with TestPipeline(options=self.pipeline_opts) as p:
        output = (
            p
            | beam.Create(records)
            | GroupIntoBatches(batch_size=2)
            | WriteToSqs(queue_name=self.queue_name)
        )
        assert_that(output[None], equal_to([]))
```

5. Failed records after all trials are returned by a tagged output. We can configure the number of trials (`max_trials`) and whether to append error details (`append_error`).

```python
class TestRetryLogic(unittest.TestCase):
    # default failed output name
    failed_output = "write-to-sqs-failed-output"

    def test_write_to_sqs_retry_no_failed_element(self):
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(4)]
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(records)
                | BatchElements(min_batch_size=4)
                | WriteToSqs(
                    queue_name="test-sqs-queue",
                    max_trials=3,
                    append_error=True,
                    fake_config={"num_success": 2},
                )
            )
            assert_that(output[None], equal_to([]))

    def test_write_to_sqs_retry_failed_element_without_appending_error(self):
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(4)]
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(records)
                | BatchElements(min_batch_size=4)
                | WriteToSqs(
                    queue_name="test-sqs-queue",
                    max_trials=3,
                    append_error=False,
                    fake_config={"num_success": 1},
                )
            )

            assert_that(
                output[self.failed_output],
                equal_to([{"Id": "3", "MessageBody": "3"}]),
            )

    def test_write_to_sqs_retry_failed_element_with_appending_error(self):
        records = [{"Id": str(i), "MessageBody": str(i)} for i in range(4)]
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(records)
                | BatchElements(min_batch_size=4)
                | WriteToSqs(
                    queue_name="test-sqs-queue",
                    max_trials=3,
                    append_error=True,
                    fake_config={"num_success": 1},
                )
            )
            assert_that(
                output[self.failed_output],
                equal_to(
                    [
                        {
                            "Id": "3",
                            "MessageBody": "3",
                            "error": {
                                "Id": "3",
                                "SenderFault": False,
                                "Code": "error-code",
                                "Message": "error-message",
                            },
                        }
                    ]
                ),
            )
```