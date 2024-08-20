---
title: Firehose PyIO Connector 0.2.0
date: 2024-08-22
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

We are happy to present the [Apache Beam Python I/O connector for Amazon Data Firehose](https://github.com/beam-pyio/firehose_pyio) 0.2.0 release.

♻️UPDATE

- Return failed elements by a tagged output, which allows users to determine how to handle them subsequently.
- Provide options that handle failed records.
  - _max_trials_ - The maximum number of trials when there is one or more failed records.
  - _append_error_ - Whether to append error details to failed records.

<!--more-->

See [this post](/blog/2024/firehose-pyio-intro/) for more examples.
