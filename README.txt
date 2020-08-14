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

In Dafka the communication between clients is done with a simple,
high-performance, language and transport agnostic protocol. This protocol is
versioned and maintains backwards compatibility with older version. We provide
a C and Java client for Dafka.

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

Partitions are distributed to the Dafka Cluster which consists of Dafka Stores.
Each partition is replicated to each store for fault tolerance.

### Producer

Producers publish records to a topic. Each producer creates its own partition
that only it publishes to. Records are send directly to *stores* and
*consumers*. When a producer goes offline its partition is still available to
consumers from the Dafka stores.

### Consumer

Consumers subscribe to a topic. Each consumer will receive records published to
that topic from all partitions.

### Tower

Each Dafka cluster has one or more towers. The towers are used to connect
producer, consumers and stores to each other. At this point no traffic is
proxied through the towers.

### Guarantees

Dafka gives the following guarantees:

* Records sent by a producer are appended in the stores in the same order they
  are sent.
* Consumers will provide records of a partition to the user in the same order
  they are sent by the producer.

## Design

We designed Dafka to be a drop-in replacement for Apache Kafka.

While Kafka makes it easy for consumers to come and go as they like, their
consumer group feature which relies on finding consensus in a group of peers
makes joining very expensive. It can take seconds before a consumer is ready to
consume records. The same is true for producer. Dafka tries to avoid finding
consensus and therefore intentionally avoids features like consumer groups in
favor of higher throughput, lower latency as well as faster consumer and
producer initialization.

This design section discusses the different message types of the Dafka protocol.

### Producing and Storing

Producers published records using the RECORD message type. RECORD messages are send
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
                          Publish | RECORD
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
is not necessarily required. When a new consumer joins they can request the
producers to supply all already published records. Therefore the producer must
store all published records that are not stored by a configurable minimum
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

### Subscribing to topics

Consumer will only start listening for HEAD message once they subscribed for
a topic. Whenever a new subscription created by a consumer it is not enough to
listen to producers HEAD messages to catch up upon the current offset of their
partition. For one there's a time penalty until producers HEAD intervals
triggered and more severe a producer may already have disappeared. Hence
consumers will send a GET-HEADS message to the stores to request the offset for
each partition they stored for a topic.

[diagram]
                    +------------+
                    |  Consumer  |
                    +------------+
                    |    PUB     |
                    \-----+------/
                          |
                 GET-HEADS|for all partitions
                          |
        +-----------------+---------------+
        |                 |               |
        |                 |               |
        v                 v               v
  /-----+-----\     /-----+-----\   /-----+-----\
  |    SUB    |     |    SUB    |   |    SUB    |
  +-----------+ ... +-----------+   +-----------+
  |  Store 1  |     |  Store 2  |   |  Store n  |
  +-----------+     +-----------+   +-----------+
[/diagram]

As a response each stores will answer with DIRECT-HEAD messages each containing
the offset for a partition.

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
        | Send DIRECT-HEAD|for each stored|partition
        |                 |               |
        |                 |               |
  /-----+-----\     /-----+-----\   /-----+-----\
  |    PUB    |     |    PUB    |   |    PUB    |
  +-----------+ ... +-----------+   +-----------+
  |  Store 1  |     |  Store 2  |   |  Store n  |
  +-----------+     +-----------+   +-----------+
[/diagram]

### Missed records

Consumer can discover missed records by either receiving HEAD messages or
receiving a RECORD messages with a higher offset than they currently have for
a certain partition. In order to fetch missed messages consumers send a FETCH
message to all connected stores and the producer of that message to request the
missed messages.

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

As a response to a FETCH message a store and/or producer may send all missed records
that the consumer requested directly to the consumer with the DIRECT-RECORD.

[diagram]
                    +------------+
                    |  Consumer  |
                    +------------+
                    |    SUB     |
                    \-----+------/
                          ^
                          |

        +-----------------+---------------+
        |                 |               |
        |     Send missing|message (DIRECT|-RECORD)
        |                 |               |
  /-----+-----\     /-----+-----\   /-----+------\
  |    PUB    |     |    PUB    |   |    PUB     |
  +-----------+ ... +-----------+   +------------+
  |  Store 1  |     |  Store n  |   | Producer x |
  +-----------+     +-----------+   +------------+
[/diagram]

## Implementation

The implementation is documented in RFC
[46/DAFKA](https://rfc.zeromq.org/spec/46)

## Ownership and License

The contributors are listed in AUTHORS. This project uses the MPL v2 license, see LICENSE.

Dafka uses the [C4.1 (Collective Code Construction Contract)](http://rfc.zeromq.org/spec:22) process for contributions.

Dafka uses the [CLASS (C Language Style for Scalability)](http://rfc.zeromq.org/spec:21) guide for code style.

To report an issue, use the [Dafka issue tracker](https://github.com/zeromq/dafka/issues) at github.com.

## Using Dafka

### Building and Installing on Linux and macOS

To start with, you need at least these packages:
* `git-all` -- git is how we share code with other people.
* `build-essential`, `libtool`, `pkg-config` - the C compiler and related tools.
* `autotools-dev`, `autoconf`, `automake` - the GNU autoconf makefile generators.
* `cmake` - the CMake makefile generators (an alternative to autoconf).

Plus some others:
* `uuid-dev`, `libpcre3-dev` - utility libraries.
* `valgrind` - a useful tool for checking your code.
* `pkg-config` - an optional useful tool to make building with dependencies easier.

Which we install like this (using the Debian-style apt-get package manager):

    sudo apt-get update
    sudo apt-get install -y \
        git-all build-essential libtool \
        pkg-config autotools-dev autoconf automake cmake \
        uuid-dev libpcre3-dev valgrind

    # only execute this next line if interested in updating the man pages as well (adds to build time):
    sudo apt-get install -y asciidoc

Here's how to build DAFKA from GitHub (building from packages is very similar, you don't clone a repo but unpack a tarball), including the libzmq (ZeroMQ core) library (NOTE: skip ldconfig on OSX):

    git clone git://github.com/zeromq/libzmq.git
    cd libzmq
    ./autogen.sh
    # do not specify "--with-libsodium" if you prefer to use internal tweetnacl security implementation (recommended for development)
    ./configure --with-libsodium
    make check
    sudo make install
    sudo ldconfig
    cd ..

    git clone git://github.com/zeromq/czmq.git
    cd czmq
    ./autogen.sh && ./configure && make check
    sudo make install
    sudo ldconfig
    cd ..

    git clone git://github.com/zeromq/dafka.git
    cd dafka
    ./autogen.sh && ./configure && make check
    sudo make install
    sudo ldconfig
    cd ..

To verify everything got installed correctly run:

    make check

### Quickstart

If you are interested in getting started with Dafka follow the instructions below.

#### Step 1: Start the Dafka Tower Deamon

We'll start to use dafka in a simple single producer/single consumer scenario using the `dafka_console_producer` and `dafka_console_consumer` commandline utilities.

```sh
$ dafka_towerd
```

Note: The tower will open two sockets. One on port 5556 to get notified by joining peers and one on port 5557 to notify joined peers about joining peers.

#### Step 2: Write some events into a topic

Run the console producer to write a few events into the `hello` topic. Each line you enter will result in a separate event being written to the topic.

```sh
$ dafka_console_producer hello
A first event
A second event
```

You can stop the producer client with Ctrl-C at any time.

Note: If no configuration is provided the producer tries to connect to a tower on localhost.

#### Step 3: Read the events

Open another terminal session and run the console consumer to read the events you just created:

```sh
$ dafka_console_consumer hello
```

You can stop the consumer client with Ctrl-C at any time.

Because events are kept by the producer until at least one store acknowledges they're stored, they can be read as many times and by as many consumers as you want. You can easily verify this by opening yet another terminal session and re-running the previous command again.

Note: If no configuration is provided the consumer tries to connect to a tower on localhost.

### Getting started

The following getting started will show you how to use the producer and consumer API.

First we construct a dafka producer with default configuration and then publish the message `HELLO WORLD` to the topic `hello`.

```c
zconfig_t *config = zconfig_new ("root", NULL);
const char *topic = "hello";

dafka_producer_args_t producer_args =  { topic, config };
zactor_t *producer = zactor_new (dafka_producer, &producer_args);

dafka_producer_msg_t *msg = dafka_producer_msg_new ();
dafka_producer_msg_set_content (msg, "HELLO WORLD");
dafka_producer_msg_send (msg, producer);

dafka_producer_msg_destroy (&msg);
zactor_destroy (&producer);
zconfig_destroy (&config);
```

To consume this message we constuct a dafka consumer, let it subscribe to topic `hello`, receive the message and then print the content of received message.

```c
zconfig_t *config = zconfig_new ("root", NULL);
const char *topic = "hello";

zactor_t *consumer = zactor_new (dafka_consumer, config);
dafka_consumer_subscribe (consumer, topic);

dafka_consumer_msg_t *msg = dafka_consumer_msg_new ();
while (true) {
    rc = dafka_consumer_msg_recv (msg, consumer);
    if (rc == -1)
        break;      // Interrupted

    char *content_str = dafka_consumer_msg_strdup (msg);
    printf ("%s\n", content_str);
    zstr_free (&content_str);
}

dafka_consumer_msg_destroy (&msg);
zactor_destroy (&consumer);
zconfig_destroy (&config);
```

### Linking with an Application

Include `dafka.h` in your application and link with libdafka. Here is a typical gcc link command:

    gcc myapp.c -o myapp -ldafka -lczmq -lzmq

### API v1 Summary

This is the API provided by Dafka v1.x, in alphabetical order.

.pull doc/dafka_consumer.doc
.pull doc/dafka_producer.doc
.pull doc/dafka_store.doc
.pull doc/dafka_tower.doc

## Contributing

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
* Run the 'selftest' script, which uses the Valgrind memcheck tool or AdressSanatizer.
* Repeat until perfect.

To run the 'selftest' script with autotools:

```sh
make check
make memcheck
```

To run the 'selftest' script with cmake:

```sh
make test or ctest
ctest -T memcheck
```

### Hints to Contributors

Don't include system headers in source files. The right place for these is dafka_prelude.h. If you need to check against configured libraries and/or headers, include platform.h in the source before including dafka.h.

Do read your code after you write it and ask, "Can I make this simpler?" We do use a nice minimalist and yet readable style. Learn it, adopt it, use it.

Before opening a pull request read our [contribution guidelines](https://github.com/zeromq/dafka/blob/master/CONTRIBUTING.md). Thanks!

### Code Generation

We generate scripts for build systems like autotools, cmake and others as well as class skeletons, class headers, the selftest runner, bindings to higher level languages and more using zproject. Generated files will have a header and footer telling you that this file was generated. To re-generate those files it is recommended to use the latest `zeromqorg/zproject` docker image.

#### Docker

* Clone [libzmq](https://github.com/zeromq/libzmq) into the same directory as dafka.
* Clone [czmq](https://github.com/zeromq/czmq) into the same directory as dafka.

Next always download the latest image:

```sh
# Make sure
docker pull zeromqorg/zproject:latest
```

Then run the following command:

```sh
# Shell and Powershell
docker run -v ${PWD}/..:/workspace -e BUILD_DIR=/workspace/czmq zeromqorg/zproject

# Windows CMD
docker run -v %cd%/..:/workspace -e BUILD_DIR=/workspace/czmq zeromqorg/zproject
```

### This Document
