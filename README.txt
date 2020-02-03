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

### Node types

Dafka defines three types of nodes:

* Producers
* Consumers
* Stores

### Node Identification and Life-cycle

A Dafka node represents either a producer, consumer or store. A Dafka node is
identified by a 16-octet universally unique identifier (UUID). Dafka does not
define how a node is created or destroyed but does assume that nodes have
a certain durability.

### Node Discovery and Presence

TBD

### Topics, Partitions and Records

Dafka version 1 defines the following regulations for topics, partitions and
records.

A Dafka cluster SHALL not place any restriction on the total number of topics
other than physicals due to the amount of available memory or disk space. The
name of a Dafka topic can be any byte sequence of arbitrary length. Though it is
RECOMMENDED to use only characters from the ASCII character set.

Each Dafka topic SHALL consist of at least one partition. In order to keep the
protocol simple each producer MUST publish records to exactly one partition and
further only one producer SHALL ever publish to the same partition. Which means
one partition can be mapped to exactly one producer. In order to make this
connection obvious the name of the partition SHALL be the producer's node UUID.

The first record of each partition SHALL have the offset 0. Each following
record's offset SHALL be the previous record's offset incremented by 1.

Dafka does not make any restrictions on the size of a record other than the
amount of memory available in the nodes.

### Interconnection Model

Dafka establishes a mesh network between all its nodes using a publish and
subscribe pattern.

Each node SHALL create a ZeroMQ PUB and a ZeroMQ SUB socket to communicate with
the towers. The PUB socket SHALL be connected to at least one tower's SUB socket
and the SUB socket SHALL be connected to at least one tower's PUB socket.

Each node SHALL create a ZeroMQ XPUB socket and bind it to an address that can
be reached by the other nodes. The node SHALL send this address at a regular
interval to all connected towers. The XPUB socket is used to send messages to
other nodes and get notified about subscriptions.

Each node SHALL create a second ZeroMQ SUB socket. When a node discovers another
node, it SHALL connect this socket, to the other nodes XPUB socket. This SUB
socket is used to receive messages from other nodes.

A node MAY disconnect its SUB socket if the peer has failed to respond within
some time (see Heartbeating).

### Protocol Signature

Every Dafka message sent SHALL start with the ZRE protocol signature, %xAA %xA0.
A node SHALL silently discard any message received that does not start with
these two octets.

This mechanism is designed particularly for applications that bind to ephemeral
ports which may have been previously used by other protocols, and to which there
are still nodes attempting to connect. It is also a general fail-fast mechanism
to detect ill-formed messages.

### Versioning

A version number octet %x01 shall follow the signature. A node SHALL discard
messages that do not contain a valid version number. There is no mechanism for
backwards interoperability.

### Protocol Grammar

.pull src/dafka_proto.bnf

### Dafka Commands

All commands start with a protocol signature (%xAA %xA5), then a command
identifier and then the protocol version number (%x30).

Each command MUST contain as first frame the name of the ZeroMQ topic it is
published to. This frame is call topic frame.

#### The STORE-HELLO Command

When a store receives a store hello subscription on its XPUB socket it SHALL
send a STORE-HELLO command to the subscriber. The STORE-HELLO command has one
field: the address of the store.

The XPUB's topic frame is set to the STORE-HELLO command's ID concatenated with
the address of the consumer received by the subscription message.

#### The CONSUMER-HELLO Command

When a consumer receives a STORE-HELLO command it SHOULD reply with
a CONSUMER-HELLO command. The CONSUMER-HELLO command has two fields: the address
of the consumer and a list of dafka topics it is subscribed to. If the consumer
is not subscribed to any dafka topic it MUST NOT send a CONSUMER-HELLO command.

The XPUB's topic frame is set to the CONSUMER-HELLO command's ID concatenated
with the address of the store the received by the STORE-HELLO command.

#### The RECORD Command

When a producer wishes to publish a record it SHALL use the RECORD command. The
RECORD command contains three fields: the dafka topic this record is published
to, the offset of this record in the partition and the records content defined
as one ZeroMQ frame. Dafka does not support multi-frame message contents.

A producer MUST NOT delete send records before receiving at least one ACK
command.

The XPUB's topic frame is set to the RECORD command's ID concatenated with the
dafka topics name the record is published to.

#### The HEAD Command

After a producer published its first record it SHALL send the HEAD command at
regular a interval. The HEAD command contains two fields: the dafka topic this
producers publishes records to and the offset of the last published record.

The XPUB's topic frame is set to the HEAD command's ID concatenated with the
address of the producer (partition).

#### The FETCH Command

A consumer or store SHALL send the FETCH command if it detects it missed
a record in a partition. The FETCH command contains three fields: the dafka
topic it missed the topic, the offset first missed record and the count of
missed records.

The XPUB's topic frame is set to the FETCH command's ID concatenated with the
consumers address.

#### The DIRECT-RECORD Command

Upon receiving a FETCH command a producer or store SHALL check if it has stored
the requested record(s). In case a requested record is available it SHALL send
a DIRECT-RECORD command. The DIRECT-RECORD command contains three fields: the
dafka topic this record was published to, the offset of the record in the
partition and the records content defined as one ZeroMQ frame.

The records requested by the FETCH command SHALL be send according to their
offset in ascending order.

The XPUB's topic frame is set to the DIRECT-RECORD command's ID concatenated
with the address of the producer or store.

#### The GET-HEADS Command

If a consumer subscribes to a dafka topic it SHALL sent a GET-HEADS command.

The XPUB's topic frame is set to the GET-HEADS command's ID concatenated with
the address of the consumer.

#### The DIRECT-HEAD Command

After receiving a GET-HEADS command a producer SHALL send a DIRECT-HEAD command
if it publishes records to dafka topic requested by the GET-HEADS command.

After receiving a GET-HEADS command a store SHALL send one DIRECT-HEAD command
for each partition it has records stored on the requested dafka topic by the
GET-HEADS command.

The XPUB's topic frame is set to the DIRECT-HEAD command's ID concatenated with
address of the producer or store.

### ZeroMQ Subscriptions

Because Dafka nodes are connected through PUB and SUB sockets each node MUST
register subscriptions with the other nodes in order to receive messages. The
following sections describe which node type requires which subscriptions.

#### Producer Subscriptions

A producer SHALL subscribe to ACK commands for its own partition by
concatenating the ACK command ID and the partition name which equals the
producer's address.

A producer SHALL subscribe to FETCH commands for its own partition by
concatenating the FETCH command ID and the partition name which equals the
producer's address.

#### Consumer Subscriptions

A consumer SHALL subscribe to DIRECT-RECORD commands addressed to it by
concatenating the DIRECT-RECORD command ID and its consumer address.

A consumer SHALL subscribe to DIRECT-HEAD commands addressed to it by
concatenating the DIRECT-HEAD command ID and its consumer address.

A consumer SHALL subscribe to STORE-HELLO commands by concatenating the
STORE-HELLO command ID and the consumer address.

For each dafka topic a consumer is subscribed to it SHALL subscribe to RECORD
commands by concatenating the RECORD command ID and topic name.

For each dafka topic a consumer is subscribed to it SHALL subscribe to HEAD
commands by concatenating the HEAD command ID and topic name.

For each partition of each dafka topic a consumer is subscribed to it MAY
subscribe to ACK commands by concatenating the ACK command ID and the partition
name which equals the producer's address.

#### Store Subscriptions

A store SHALL subscribe to all RECORD commands by subscribing the RECORD command
ID.

A store SHALL subscribe to all HEAD commands by subscribing the HEAD command ID.

A store SHALL subscribe to DIRECT-RECORD commands addressed to it by
concatenating the DIRECT-RECORD command ID and its store address.

A store SHALL subscribe to all FETCH commands by subscribing to the FETCH
command ID.

A store SHALL subscribe to GET-HEADS commands by subscribing to the GET-HEADS
command ID.

A store SHALL subscribe to CONSUMER-HELLO commands by subscribing to the
CONSUMER-HELLO command ID

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
