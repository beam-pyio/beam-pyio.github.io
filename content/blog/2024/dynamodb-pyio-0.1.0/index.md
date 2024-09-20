---
title: DynamoDB PyIO Connector 0.1.0
date: 2024-09-24
draft: false
featured: false
comment: false
toc: false
reward: false
pinned: false
carousel: false
featuredImage: false
series:
tags:
categories:
  - announcement
tags: 
authors:
  - JaehyeonKim
images: []
# description: To be updated...
---

We are happy to present the first release of the [Apache Beam Python I/O connector for Amazon DynamoDB](https://github.com/beam-pyio/dynamodb_pyio).

✨NEW

- Add a composite transform (`WriteToDynamoDB`) that writes records to a DynamoDB table with help of the [`batch_writer`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/batch_writer.html) of the boto3 package.
  - The batch writer will automatically handle buffering and sending items in batches. In addition, it will also automatically handle any unprocessed items and resend them as needed.


<!--more-->

- Provide an option that handles duplicate records
  - *dedup_pkeys* - List of keys to be used for de-duplicating items in buffer.
- Create a dedicated pipeline option (`DynamoDBOptions`) that reads AWS related values (e.g. `aws_access_key_id`) from pipeline arguments.
- Implement a metric object that records the total counts.
- Add unit and integration testing cases. The [moto](https://github.com/getmoto/moto) and [localstack-utils](https://docs.localstack.cloud/user-guide/tools/testing-utils/) are used for unit and integration testing respectively.
- Integrate with GitHub Actions by adding workflows for testing, documentation and release management.

See [Introduction to DynamoDB PyIO Sink Connector](/blog/2024/dynamodb-pyio-intro/) for more examples.
