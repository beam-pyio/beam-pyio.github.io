---
title: Firehose PyIO Connector 0.1.0
date: 2024-07-19
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

We are happy to present the first release of the [Apache Beam Python I/O connector for Amazon Data Firehose](https://github.com/beam-pyio/firehose_pyio).

✨NEW

- Add a composite transform (`WriteToFirehose`) that puts records into a Firehose delivery stream in batch, using the [`put_record_batch`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/firehose/client/put_record_batch.html) method of the boto3 package.


<!--more-->

- Provide options that control individual records and handle failed records.
- Create a dedicated pipeline option (`FirehoseOptions`) that reads AWS related values (e.g. `aws_access_key_id`) from pipeline arguments.
- Implement metric objects that record the total, succeeded and failed elements counts.
- Add unit and integration testing cases. The [moto](https://github.com/getmoto/moto) and [localstack-utils](https://docs.localstack.cloud/user-guide/tools/testing-utils/) packages are used for unit and integration testing respectively. Also, a custom test client is created for testing retry of failed elements, which is not supported by the moto package.

See [this post](/blog/2024/firehose-pyio-intro/) for more examples.
