.set GIT=https://github.com/zeromq/dafka
.sub 0MQ=ØMQ

[![GitHub release](https://img.shields.io/github/release/zeromq/dafka.svg)](https://github.com/zeromq/dafka/releases)
<a target="_blank" href="http://webchat.freenode.net?channels=%23zeromq&uio=d4"><img src="https://cloud.githubusercontent.com/assets/493242/14886493/5c660ea2-0d51-11e6-8249-502e6c71e9f2.png" height = "20" /></a>
[![license](https://img.shields.io/badge/license-MPLV2.0-blue.svg)](https://github.com/zeromq/dafka/blob/master/LICENSE)

# Dafka - Decentralized Distributed Streaming Platform

[![Build Status](https://travis-ci.org/zeromq/dafka.png?branch=master)](https://travis-ci.org/zeromq/dafka)

## Contents

.toc 2

## Overview

### Scope and Goals

Dafka is a decentralize distributed streaming platform. What exactly does that
mean?

A streaming platform has three key capabilities:

* Publish and subscribe to streams of records, similar to a message queue or
  enterprise messaging system.
* Store streams of records in a fault-tolerant durable way.
* Process streams of records as they occur.

Dafka is generally used for two broad classes of applications:

* Building real-time streaming data pipelines that reliably get data between
  systems or applications
* Building real-time streaming applications that transform or react to the
  streams of data

To understand how Dafka does these things, let's dive in and explore Dafka's
capabilities from the bottom up.

First a few concepts:

* Dafka is run as a cluster on one or more servers.
* The Dafka cluster stores streams of records in categories called topics.
* Each record consists of a arbitrary value.
* Producers send record to the Cluster and directly to the Consumers.
* Missed records are obtained either from the Producer or the Cluster.

In Dafka the communication between the clients is done with a simple,
high-performance, language and transport agnostic protocol. This protocol is
versioned and maintains backwards compatibility with older version. We provide
a C client for Dafka.

### Topics and Partitions

Dafka provides an abstraction for records called topic.

A topic is a name to which records are published. Topics in Dafka are always
multi-subscriber; that is, a topic can have zero, one, or many consumers that
subscribe to the records written to it.

Each Dafka topic consists of at least one partitions that looks like this:

[diagram]
                Structure of a Topic

              +---+---+---+---+---+---+---+
  Partition 1 | 0 | 1 | 2 | 3 | 4 | 5 | 6 : <------------=-- Writes
              +---+---+---+---+---+---+---+

              +---+---+---+---+---+---+---+---+---+
  Partition 2 | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 : <----=-- Writes
              +---+---+---+---+---+---+---+---+---+

              +---+---+---+---+---+---+
  Partition 3 | 0 | 1 | 2 | 3 | 4 | 5 : <----------------=-- Writes
              +---+---+---+---+---+---+

         time ------------------------------------>
[/diagram]

Each partition is an ordered, immutable sequence of records that is continually
appended to. The records in the partitions are each assigned a sequential id
number called the offset that uniquely identifies each record within the
partition.

The Dafka cluster durably persists all published records — whether or not they
have been consumed.

[diagram]
                       Producer
                          |
                          | writes
                          v
+---+---+---+---+---+---+-+-+
| 0 | 1 | 2 | 3 | 4 | 5 | 6 :
+---+---+-+-+---+---+-+-+---+
          |           |
          |   reads   |
          v           v
      Consumer 1  Consumer 2
      (offset=2)  (offset=5)
[/diagram]

Consumers maintain their own offset while reading records of a partition. In fact
neither the Dafka Cluster nor the producers keep track of the consumers offset.
This design allows Consumer to either reset their offset to an older offset and
re-read records or set their offset to a newer offset and skip ahead.

In that way consumer have no influence on the cluster, the producer and other
consumers. They simply can come and go as they please.

### Stores

Partition are distributed to the Dafka Cluster which consists of Dafka Stores.
Each partition is replicated to each store for fault tolerance.

### Producer

Producers publish records to a topic. Each producer creates its own partition
that only it publishes to. Records are send directly to *stores* and
*consumers*. When a producer goes offline its partition is still available to
consumers from the dafka stores.

### Consumer

Consumers subscribe to a topic. Each consumer will receive records published to
that topic from all partitions.

### Guarantees

Dafka gives the following guarantees:

* Records sent by a producer are appended in the stores in the same order they
  are sent.
* Consumers will provide records of a partition to the user in the same order
  they are sent by the producer.

## Design

We designed Dafka the be a drop-in replacement for Apache Kafka.

While Kafka makes it easy for consumers to come and go as they like their
consumer group feature which relies on finding consensus in a group of peers
makes joining very expensive. It can take seconds before a consumer ready to
consume records. The same is true for producer. Dafka tries to avoid finding
consensus and perform leader election and therefore Dafka intentionally avoids
features like consumer groups in favor of higher throughput, lower latency as
well as faster consumer and producer initialization.

This design section discusses the different message types of the Dafka protocol.

### Producing and Storing

Producers published records using the MSG message type. MSG messages are send
directly to all connected stores as well as all connected consumers. Once
a producer published its first records it starts sending HEAD messages at
a regular interval informing both stores and consumer about the last published
records which gives stores and consumers a chance to figure out whether or not
the missed one or more records.

[diagram]
                            +------------+
                            |  Producer  |
                            +------------+
                            |    PUB     |
                            \-----+------/
                                  |
                          Publish | MSG
                          Publish | HEAD (interval)
                                  |
        +-----------------+-------+-------+------------------+
        |                 |               |                  |
        |                 |               |                  |
        v                 v               v                  v
  /-----+-----\     /-----+-----\   /-----+------\     /-----+------\
  |    SUB    |     |    SUB    |   |    SUB     |     |    SUB     |
  +-----------+ ... +-----------+   +------------+ ... +------------+
  |  Store 1  |     |  Store n  |   | Consumer 1 |     | Consumer m |
  +-----------+     +-----------+   +------------+     +------------+
[/diagram]

Because producers publish records directly to consumers the presence of a store
is not necessarily required. When a new consumer joins producers must supply all
already published records to that new consumer. Therefore the producer must
store a all published records that are not stored by a configurable minimum
number stores. To inform a producer about the successful storing of a records
the stores send a ACK message to the producer.

[diagram]
           +------------+
           |  Producer  |
           +------------+
           |    PUB     |
           \-----+------/
                 ^
                 |
                 |
        +--------+--------+
        |                 |
    ACK |                 | ACK
        |                 |
  /-----+-----\     /-----+-----\
  |    SUB    |     |    SUB    |
  +-----------+ ... +-----------+
  |  Store 1  |     |  Store n  |
  +-----------+     +-----------+
[/diagram]

### Missed messages

Consumer discover missed messages by receiving HEAD messages. In order to fetch
missed messages consumer send a FETCH message to all connected stores and the
producer to request the number of missed messages.

[diagram]
                    +------------+
                    |  Consumer  |
                    +------------+
                    |    PUB     |
                    \-----+------/
                          |
                 FETCH mis|sing messages
                          |
        +-----------------+---------------+
        |                 |               |
        |                 |               |
        v                 v               v
  /-----+-----\     /-----+-----\   /-----+------\
  |    SUB    |     |    SUB    |   |    SUB     |
  +-----------+ ... +-----------+   +------------+
  |  Store 1  |     |  Store n  |   | Producer x |
  +-----------+     +-----------+   +------------+
[/diagram]

As a response to a FETCH message a store or producer may send all missed records
that the consumer requested.

[diagram]
                    +------------+
                    |  Consumer  |
                    +------------+
                    |    SUB     |
                    \-----+------/
                          ^
                          |
                          |
        +-----------------+---------------+
        |                 |               |
        |     Send missing|message        |
        |                 |               |
  /-----+-----\     /-----+-----\   /-----+------\
  |    PUB    |     |    PUB    |   |    PUB     |
  +-----------+ ... +-----------+   +------------+
  |  Store 1  |     |  Store n  |   | Producer x |
  +-----------+     +-----------+   +------------+
[/diagram]

### Dead producer

To be continued ...

## Implementation

### Ownership and License

The contributors are listed in AUTHORS. This project uses the MPL v2 license, see LICENSE.

Dafka uses the [C4.1 (Collective Code Construction Contract)](http://rfc.zeromq.org/spec:22) process for contributions.

Dafka uses the [CLASS (C Language Style for Scalability)](http://rfc.zeromq.org/spec:21) guide for code style.

To report an issue, use the [Dafka issue tracker](https://github.com/zeromq/dafka/issues) at github.com.

## Using Dafka

### Building and Installing

To start with, you need at least these packages:

* {{git-all}} -- git is how we share code with other people.

* {{build-essential}}, {{libtool}}, {{pkg-config}} - the C compiler and related tools.

* {{autotools-dev}}, {{autoconf}}, {{automake}} - the GNU autoconf makefile generators.

* {{cmake}} - the CMake makefile generators (an alternative to autoconf).

Plus some others:

* {{uuid-dev}}, {{libpcre3-dev}} - utility libraries.

* {{valgrind}} - a useful tool for checking your code.

* {{pkg-config}} - an optional useful tool to make building with dependencies easier.

Which we install like this (using the Debian-style apt-get package manager):

    sudo apt-get update
    sudo apt-get install -y \
        git-all build-essential libtool \
        pkg-config autotools-dev autoconf automake cmake \
        uuid-dev libpcre3-dev valgrind

    # only execute this next line if interested in updating the man pages as well (adds to build time):
    sudo apt-get install -y asciidoc

TODO...

### Linking with an Application

Include `dafka.h` in your application and link with libdafka. Here is a typical gcc link command:

    gcc myapp.c -o myapp -ldafka -lczmq -lzmq

### API v1 Summary

This is the API provided by Dafka v1.x, in alphabetical order.

.pull doc/dafka_consumer.doc
.pull doc/dafka_producer.doc
.pull doc/dafka_store.doc
.pull doc/dafka_tower.doc

### Documentation

Man pages are generated from the class header and source files via the doc/mkman tool, and similar functionality in the gitdown tool (http://github.com/imatix/gitdown). The header file for a class must wrap its interface as follows (example is from include/zclock.h):

    //  @interface
    //  Sleep for a number of milliseconds
    void
        zclock_sleep (int msecs);

    //  Return current system clock as milliseconds
    int64_t
        zclock_time (void);

    //  Self test of this class
    int
        zclock_test (Bool verbose);
    //  @end

The source file for a class must provide documentation as follows:

    /*
    @header
    ...short explanation of class...
    @discuss
    ...longer discussion of how it works...
    @end
    */

The source file for a class then provides the self test example as follows:

    //  @selftest
    int64_t start = zclock_time ();
    zclock_sleep (10);
    assert ((zclock_time () - start) >= 10);
    //  @end

The template for man pages is in doc/mkman.

### Development

Dafka is developed through a test-driven process that guarantees no memory violations or leaks in the code:

* Modify a class or method.
* Update the test method for that class.
* Run the 'selftest' script, which uses the Valgrind memcheck tool.
* Repeat until perfect.

### Hints to Contributors

Don't include system headers in source files. The right place for these is dafka_prelude.h. If you need to check against configured libraries and/or headers, include platform.h in the source before including dafka.h.

Do read your code after you write it and ask, "Can I make this simpler?" We do use a nice minimalist and yet readable style. Learn it, adopt it, use it.

Before opening a pull request read our [contribution guidelines](https://github.com/zeromq/dafka/blob/master/CONTRIBUTING.md). Thanks!

### Code Generation

TODO

### This Document

This document is originally at README.txt and is built using [gitdown](http://github.com/imatix/gitdown).
