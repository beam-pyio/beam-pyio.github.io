---
title: Apache Beam Python I/O Connectors
date: 2024-07-04
draft: false
featured: false
comment: false
toc: false
reward: false
pinned: true
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

[Apache Beam](https://beam.apache.org/) is an open source unified programming model to define and execute data processing pipelines, including ETL, batch and stream processing. We consider it has a huge potential to improve traditional development patterns in both transactional and analytical processing of data. Specifically it can be applied to event-driven applications, data pipelines and streaming analytics.

Employing [dataflow programming](https://en.wikipedia.org/wiki/Dataflow_programming), Beam supports a range of [I/O connectors](https://beam.apache.org/documentation/io/connectors/), but we find some gaps in the existing connectors especially in relation to the Python SDK. It fueled us to start the [Apache Beam Python I/O Connectors](https://github.com/beam-pyio) project.

<!--more-->

As long time AWS users, we see connectors for some key AWS services are missing or are not available in the Python SDK. Those services cover Firehose, SQS, SNS, DynamoDB and EventBridge, and we have started developing connectors for them.

In data engineering projects, OLTP/OLAP systems and open table formats (Apache Iceberg, Apache Hudi and Delta Lake) are key data sources and destinations (sinks). Python-native connectors can make it simpler to develop data pipelines that deals with those data storage systems, and we plan to develop relevant connectors by integrating with the [daft](https://www.getdaft.io/) package.

We keep looking into adding new connectors. If you have a new idea, please add a comment to [this discussion](https://github.com/orgs/beam-pyio/discussions/4) or check the [project repository](https://github.com/beam-pyio) for updates on new connectors.
