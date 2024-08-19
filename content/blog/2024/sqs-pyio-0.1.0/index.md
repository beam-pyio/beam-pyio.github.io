---
title: SQS PyIO Connector 0.1.0
date: 2024-08-20
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

We are happy to present the first release of the [Apache Beam Python I/O connector for Amazon SQS](https://github.com/beam-pyio/sqs_pyio).

✨NEW

- Add a composite transform (`WriteToSqs`) that sends messages to a SQS queue in batch, using the [`send_message_batch`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/send_message_batch.html) method of the boto3 package.


<!--more-->

- Provide options that handle failed records.
  - _max_trials_ - The maximum number of trials when there is one or more failed records.
  - _append_error_ - Whether to append error details to failed records.
- Return failed elements by a tagged output, which allows users to determine how to handle them subsequently.
- Create a dedicated pipeline option (`SqsOptions`) that reads AWS related values (e.g. `aws_access_key_id`) from pipeline arguments.
- Implement metric objects that record the total, succeeded and failed elements counts.
- Add unit and integration testing cases. The [moto](https://github.com/getmoto/moto) and [localstack-utils](https://docs.localstack.cloud/user-guide/tools/testing-utils/) are used for unit and integration testing respectively. Also, a custom test client is created for testing retry behavior, which is not supported by the moto package.
- Integrate with GitHub Actions by adding workflows for testing, documentation and release management.

See [this post](/blog/2024/sqs-pyio-intro/) for more examples.
