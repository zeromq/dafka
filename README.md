
[![GitHub release](https://img.shields.io/github/release/zeromq/dafka.svg)](https://github.com/zeromq/dafka/releases)
<a target="_blank" href="http://webchat.freenode.net?channels=%23zeromq&uio=d4"><img src="https://cloud.githubusercontent.com/assets/493242/14886493/5c660ea2-0d51-11e6-8249-502e6c71e9f2.png" height = "20" /></a>
[![license](https://img.shields.io/badge/license-MPLV2.0-blue.svg)](https://github.com/zeromq/dafka/blob/master/LICENSE)

# Dafka - Decentralized Distributed Streaming Platform

[![Build Status](https://travis-ci.org/zeromq/dafka.png?branch=master)](https://travis-ci.org/zeromq/dafka)

## Contents


**[Overview](#overview)**
*  [Scope and Goals](#scope-and-goals)
*  [Topics and Partitions](#topics-and-partitions)
*  [Stores](#stores)
*  [Producer](#producer)
*  [Consumer](#consumer)
*  [Tower](#tower)
*  [Guarantees](#guarantees)

**[Design](#design)**
*  [Producing and Storing](#producing-and-storing)
*  [Subscribing to topics](#subscribing-to-topics)
*  [Missed records](#missed-records)

**[Implementation](#implementation)**
*  [Node types](#node-types)
*  [Node Identification and Life-cycle](#node-identification-and-life-cycle)
*  [Node Discovery and Presence](#node-discovery-and-presence)
*  [Topics, Partitions and Records](#topics-partitions-and-records)
*  [Interconnection Model](#interconnection-model)
*  [Protocol Signature](#protocol-signature)
*  [Versioning](#versioning)
*  [Protocol Grammar](#protocol-grammar)
*  [Dafka Commands](#dafka-commands)
&emsp;[The STORE-HELLO Command](#the-store-hello-command)
&emsp;[The CONSUMER-HELLO Command](#the-consumer-hello-command)
&emsp;[The RECORD Command](#the-record-command)
&emsp;[The HEAD Command](#the-head-command)
&emsp;[The FETCH Command](#the-fetch-command)
&emsp;[The DIRECT-RECORD Command](#the-direct-record-command)
&emsp;[The GET-HEADS Command](#the-get-heads-command)
&emsp;[The DIRECT-HEAD Command](#the-direct-head-command)
*  [ZeroMQ Subscriptions](#zeromq-subscriptions)
&emsp;[Producer Subscriptions](#producer-subscriptions)
&emsp;[Consumer Subscriptions](#consumer-subscriptions)
&emsp;[Store Subscriptions](#store-subscriptions)
*  [Ownership and License](#ownership-and-license)

**[Using Dafka](#using-dafka)**
*  [Building and Installing](#building-and-installing)
*  [Linking with an Application](#linking-with-an-application)
*  [API v1 Summary](#api-v1-summary)
&emsp;[dafka_consumer - no title found](#dafka_consumer---no-title-found)
&emsp;[dafka_producer - no title found](#dafka_producer---no-title-found)
&emsp;[dafka_store - no title found](#dafka_store---no-title-found)
&emsp;[dafka_tower - no title found](#dafka_tower---no-title-found)
*  [Documentation](#documentation)
*  [Development](#development)
*  [Hints to Contributors](#hints-to-contributors)
*  [Code Generation](#code-generation)
*  [This Document](#this-document)

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

<center>
<img src="https://github.com/zeromq/dafka/raw/master/images/README_1.png" alt="1">
</center>

Each partition is an ordered, immutable sequence of records that is continually
appended to. The records in the partitions are each assigned a sequential id
number called the offset that uniquely identifies each record within the
partition.

The Dafka cluster durably persists all published records — whether or not they
have been consumed.

<center>
<img src="https://github.com/zeromq/dafka/raw/master/images/README_2.png" alt="2">
</center>

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

<center>
<img src="https://github.com/zeromq/dafka/raw/master/images/README_3.png" alt="3">
</center>

Because producers publish records directly to consumers the presence of a store
is not necessarily required. When a new consumer joins they can request the
producers to supply all already published records. Therefore the producer must
store all published records that are not stored by a configurable minimum
number stores. To inform a producer about the successful storing of a records
the stores send a ACK message to the producer.

<center>
<img src="https://github.com/zeromq/dafka/raw/master/images/README_4.png" alt="4">
</center>

### Subscribing to topics

Consumer will only start listening for HEAD message once they subscribed for
a topic. Whenever a new subscription created by a consumer it is not enough to
listen to producers HEAD messages to catch up upon the current offset of their
partition. For one there's a time penalty until producers HEAD intervals
triggered and more severe a producer may already have disappeared. Hence
consumers will send a GET-HEADS message to the stores to request the offset for
each partition they stored for a topic.

<center>
<img src="https://github.com/zeromq/dafka/raw/master/images/README_5.png" alt="5">
</center>

As a response each stores will answer with DIRECT-HEAD messages each containing
the offset for a partition.

<center>
<img src="https://github.com/zeromq/dafka/raw/master/images/README_6.png" alt="6">
</center>

### Missed records

Consumer can discover missed records by either receiving HEAD messages or
receiving a RECORD messages with a higher offset than they currently have for
a certain partition. In order to fetch missed messages consumers send a FETCH
message to all connected stores and the producer of that message to request the
missed messages.

<center>
<img src="https://github.com/zeromq/dafka/raw/master/images/README_7.png" alt="7">
</center>

As a response to a FETCH message a store and/or producer may send all missed records
that the consumer requested directly to the consumer with the DIRECT-RECORD.

<center>
<img src="https://github.com/zeromq/dafka/raw/master/images/README_8.png" alt="8">
</center>

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

The following ABNF grammar defines the dafka_proto:

    DAFKA           = join-consumer / publish / offsets

    join-consumer   = S:STORE-HELLO C:CONSUMER-HELLO *( S:DIRECT-HEAD [ consumer-fetch ] )

    consumer-fetch  = C:FETCH 1*( P:DIRECT-RECORD / S:DIRECT-RECORD )

    publish         = P:RECORD [ consumer-fetch / store-fetch S:ACK ]

    store-fetch     = S:FETCH 1*( ( P:DIRECT-RECORD / S:DIRECT-RECORD ) [ S:ACK ] )

    offsets         = P:HEAD [ consumer-fetch / store-fetch ]

    ;  Record from producer to consumers and stores. Topic is the name of the
    ;  topic. Subject is the name of the topic. Address is the address of the
    ;  producer (partition).

    RECORD          = signature %d'M' version address subject sequence content
    signature       = %xAA %xA5             ; two octets
    version         = number-1              ; Version = 1
    address         = string                ;
    subject         = string                ;
    sequence        = number-8              ;
    content         = frame                 ;

    ;  Direct record from a producer or a store to a consumer. Topic is the
    ;  address of the requestor. Subject is the name of the topic. Address is
    ;  the address of the producer (partition).

    DIRECT-RECORD   = signature %d'D' version address subject sequence content
    version         = number-1              ; Version = 1
    address         = string                ;
    subject         = string                ;
    sequence        = number-8              ;
    content         = frame                 ;

    ;  Consumer or store publish this message when a record is missing.
    ;  Either producer or a store can answer. Topic is the address of the
    ;  producer (partition). Subject is the name of the topic. Address is the
    ;  address of this message's sender. Count is the number of messages to
    ;  fetch starting with the record identified by sequence.

    FETCH           = signature %d'F' version address subject sequence count
    version         = number-1              ; Version = 1
    address         = string                ;
    subject         = string                ;
    sequence        = number-8              ;
    count           = number-4              ;

    ;  Ack from a stores to a producer. Topic is the address of the producer
    ;  (partition). Subject is the name of the topic.

    ACK             = signature %d'K' version address subject sequence
    version         = number-1              ; Version = 1
    address         = string                ;
    subject         = string                ;
    sequence        = number-8              ;

    ;  Head from producer to consumers and stores. Topic is the name of the
    ;  topic. Subject is the name of the topic. Address is the address of the
    ;  producer (partition).

    HEAD            = signature %d'H' version address subject sequence
    version         = number-1              ; Version = 1
    address         = string                ;
    subject         = string                ;
    sequence        = number-8              ;

    ;  Head from producer or store to a consumers. Topic is the name of the
    ;  topic. Subject is the name of the topic. Address is the address of the
    ;  producer (partition).

    DIRECT-HEAD     = signature %d'E' version address subject sequence
    version         = number-1              ; Version = 1
    address         = string                ;
    subject         = string                ;
    sequence        = number-8              ;

    ;  Get heads from stores send by a consumer. Topic is the name of the
    ;  topic. Address is the address of the consumer.

    GET-HEADS       = signature %d'G' version address
    version         = number-1              ; Version = 1
    address         = string                ;

    ;  Hello message from a consumer to a store. Topic is the store's
    ;  address. Address is the address of the consumer. Subjects is a list of
    ;  all topic the consumer is subscribed to.

    CONSUMER-HELLO  = signature %d'W' version address subjects
    version         = number-1              ; Version = 1
    address         = string                ;
    subjects        = strings               ;

    ;  Hello message from a store to a consumer. Topic is the consumer's
    ;  address. Address is the address of the store.

    STORE-HELLO     = signature %d'L' version address
    version         = number-1              ; Version = 1
    address         = string                ;

    ; A list of string values
    strings         = strings-count *strings-value
    strings-count   = number-4
    strings-value   = longstr

    ; A frame is zero or more octets encoded as a ZeroMQ frame
    frame           = *OCTET

    ; Strings are always length + text contents
    string          = number-1 *VCHAR
    longstr         = number-4 *VCHAR

    ; Numbers are unsigned integers in network byte order
    number-1        = 1OCTET
    number-4        = 4OCTET
    number-8        = 8OCTET

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

#### dafka_consumer - no title found

dafka_consumer -

TODO:
  - Send earliest message when a store connects
  - We must not send FETCH on every message, the problem is, that if you
    missed something, and there is high rate, you will end up sending a
    lot of fetch messages for same address
  - Prioritize DIRECT_MSG messages over MSG this will avoid discrding MSGs
    when catching up

This is the class interface:

```h
    //  This is a stable class, and may not change except for emergencies. It
    //  is provided in stable builds.
    //
    DAFKA_EXPORT void
        dafka_consumer (zsock_t *pipe, void *args);
    
    //
    DAFKA_EXPORT int
        dafka_consumer_subscribe (zactor_t *self, const char *subject);
    
    //  Self test of this class.
    DAFKA_EXPORT void
        dafka_consumer_test (bool verbose);
    
```
Please add '@interface' section in './../src/dafka_consumer.c'.

This is the class self test code:

```c
    zconfig_t *config = zconfig_new ("root", NULL);
    zconfig_put (config, "test/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/interval", "50");
    zconfig_put (config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/sub_address", "inproc://consumer-tower-sub");
    zconfig_put (config, "beacon/pub_address", "inproc://consumer-tower-pub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address", "inproc://consumer-tower-sub");
    zconfig_put (config, "tower/pub_address", "inproc://consumer-tower-pub");
    zconfig_put (config, "consumer/verbose", verbose ? "1" : "0");
    zconfig_put (config, "producer/verbose", verbose ? "1" : "0");
    zconfig_put (config, "store/verbose", verbose ? "1" : "0");
    zconfig_put (config, "store/db", SELFTEST_DIR_RW "/storedb");
    
    zactor_t *tower = zactor_new (dafka_tower_actor, config);
    
    // -------------------------------------------------------------------
    // Test with 'consumer.offset.reset = earliest' triggered by HEAD msg
    // -------------------------------------------------------------------
    zconfig_put (config, "consumer/offset/reset", "earliest");
    
    zactor_t *test_peer = zactor_new (dafka_test_peer, config);
    assert (test_peer);
    
    //  GIVEN a dafka consumer
    zactor_t *consumer = zactor_new (dafka_consumer, config);
    assert (consumer);
    zclock_sleep (250); // Make sure both peers are connected to each other
    
    //  WHEN consumer subscribes to topic 'hello'
    int rc = dafka_consumer_subscribe (consumer, "hello");
    assert (rc == 0);
    
    //  THEN the consumer will send a GET_HEADS msg for the topic 'hello'
    dafka_proto_t *msg = dafka_test_peer_recv (test_peer);
    assert_get_heads_msg (msg, "hello");
    
    //  WHEN a HEAD msg with sequence larger 0 is sent on topic 'hello'
    dafka_test_peer_send_head (test_peer, "hello", 1);
    
    // THEN the consumer will send a FETCH msg for the topic 'hello'
    msg = dafka_test_peer_recv (test_peer);
    assert_fetch_msg (msg, "hello", 0);
    
    //  WHEN a RECORD msg with sequence 0 and content 'CONTENT' is send on topic
    //  'hello'
    dafka_test_peer_send_record (test_peer, "hello", 0, "CONTENT");
    
    //  THEN a consumer msg is sent to the user with topic 'hello' and content
    //  'CONTENT'
    dafka_consumer_msg_t *c_msg = dafka_consumer_msg_new ();
    dafka_consumer_msg_recv (c_msg, consumer);
    assert_consumer_msg (c_msg, "hello", "CONTENT");
    
    dafka_consumer_msg_destroy (&c_msg);
    zactor_destroy (&consumer);
    zactor_destroy (&test_peer);
    
    // ---------------------------------------------------------------------
    // Test with 'consumer.offset.reset = earliest' triggered by RECORD msg
    // ---------------------------------------------------------------------
    zconfig_put (config, "consumer/offset/reset", "earliest");
    
    test_peer = zactor_new (dafka_test_peer, config);
    assert (test_peer);
    
    //  GIVEN a dafka consumer
    consumer = zactor_new (dafka_consumer, config);
    assert (consumer);
    zclock_sleep (250); //  Make sure both peers are connected to each other
    
    //  WHEN consumer subscribes to topic 'hello'
    rc = dafka_consumer_subscribe (consumer, "hello");
    assert (rc == 0);
    
    //  THEN the consumer will send a GET_HEADS msg for the topic 'hello'
    msg = dafka_test_peer_recv (test_peer);
    assert_get_heads_msg (msg, "hello");
    
    //  WHEN a RECORD msg with sequence larger 0 is sent on topic 'hello'
    dafka_test_peer_send_record (test_peer, "hello", 1, "CONTENT");
    
    // THEN the consumer will send a FETCH msg for the topic 'hello'
    msg = dafka_test_peer_recv (test_peer);
    assert_fetch_msg (msg, "hello", 0);
    
    //  WHEN a RECORD msg with sequence 0 and content 'CONTENT' is send on topic
    //  'hello'
    dafka_test_peer_send_record (test_peer, "hello", 0, "CONTENT");
    
    //  THEN a consumer msg is sent to the user with topic 'hello' and content
    //  'CONTENT'
    c_msg = dafka_consumer_msg_new ();
    dafka_consumer_msg_recv (c_msg, consumer);
    assert_consumer_msg (c_msg, "hello", "CONTENT");
    
    dafka_consumer_msg_destroy (&c_msg);
    zactor_destroy (&consumer);
    zactor_destroy (&test_peer);
    
    // ------------------------------------------------------------------
    // Test with Producer + Store and 'consumer.offset.reset = earliest'
    // ------------------------------------------------------------------
    zconfig_put (config, "consumer/offset/reset", "earliest");
    
    dafka_producer_args_t pub_args = {"hello", config};
    zactor_t *producer = zactor_new (dafka_producer, &pub_args);
    assert (producer);
    
    zactor_t *store = zactor_new (dafka_store_actor, config);
    assert (store);
    
    consumer = zactor_new (dafka_consumer, config);
    assert (consumer);
    zclock_sleep (250);
    
    dafka_producer_msg_t *p_msg = dafka_producer_msg_new ();
    dafka_producer_msg_set_content_str (p_msg, "HELLO MATE");
    rc = dafka_producer_msg_send (p_msg, producer);
    assert (rc == 0);
    zclock_sleep (100);  // Make sure message is published before consumer subscribes
    
    rc = dafka_consumer_subscribe (consumer, "hello");
    assert (rc == 0);
    zclock_sleep (250);  // Make sure subscription is active before sending the next message
    
    // This message is discarded but triggers a FETCH from the store
    dafka_producer_msg_set_content_str (p_msg, "HELLO ATEM");
    rc = dafka_producer_msg_send (p_msg, producer);
    assert (rc == 0);
    zclock_sleep (
            100);  // Make sure the first two messages have been received from the store and the consumer is now up to date
    
    dafka_producer_msg_set_content_str (p_msg, "HELLO TEMA");
    rc = dafka_producer_msg_send (p_msg, producer);
    assert (rc == 0);
    
    // Receive the first message from the STORE
    c_msg = dafka_consumer_msg_new ();
    dafka_consumer_msg_recv (c_msg, consumer);
    assert_consumer_msg (c_msg, "hello", "HELLO MATE");
    
    // Receive the second message from the STORE as the original has been discarded
    dafka_consumer_msg_recv (c_msg, consumer);
    assert_consumer_msg (c_msg, "hello", "HELLO ATEM");
    
    // Receive the third message from the PUBLISHER
    dafka_consumer_msg_recv (c_msg, consumer);
    assert_consumer_msg (c_msg, "hello", "HELLO TEMA");
    
    dafka_producer_msg_destroy (&p_msg);
    dafka_consumer_msg_destroy (&c_msg);
    zactor_destroy (&producer);
    zactor_destroy (&store);
    zactor_destroy (&consumer);
    
    // --------------------------------------------------------------
    // Test with Producer + Store and consumer.offset.reset = latest
    // --------------------------------------------------------------
    zconfig_put (config, "consumer/offset/reset", "latest");
    
    producer = zactor_new (dafka_producer, &pub_args);
    assert (producer);
    
    consumer = zactor_new (dafka_consumer, config);
    assert (consumer);
    zclock_sleep (250);
    
    //  This message is missed by the consumer and later ignored because the
    //  offset reset is set to latest.
    p_msg = dafka_producer_msg_new ();
    dafka_producer_msg_set_content_str (p_msg, "HELLO MATE");
    rc = dafka_producer_msg_send (p_msg, producer);
    assert (rc == 0);
    zclock_sleep (100);  // Make sure message is published before consumer subscribes
    
    rc = dafka_consumer_subscribe (consumer, "hello");
    assert (rc == 0);
    zclock_sleep (250);  // Make sure subscription is active before sending the next message
    
    dafka_producer_msg_set_content_str (p_msg, "HELLO ATEM");
    rc = dafka_producer_msg_send (p_msg, producer);
    assert (rc == 0);
    
    // Receive the second message from the PRODUCER
    c_msg = dafka_consumer_msg_new ();
    dafka_consumer_msg_recv (c_msg, consumer);
    assert_consumer_msg (c_msg, "hello", "HELLO ATEM");
    
    // We have to create a store in-order to ack all publisher messages and allow the publisher to terminate
    store = zactor_new (dafka_store_actor, config);
    assert (store);
    
    dafka_producer_msg_destroy (&p_msg);
    dafka_consumer_msg_destroy (&c_msg);
    zactor_destroy (&tower);
    zactor_destroy (&producer);
    zactor_destroy (&store);
    zactor_destroy (&consumer);
    zconfig_destroy (&config);
```

#### dafka_producer - no title found

dafka_publisher -

Please add '@discuss' section in './../src/dafka_producer.c'.

This is the class interface:

```h
    //  This is a stable class, and may not change except for emergencies. It
    //  is provided in stable builds.
    //
    DAFKA_EXPORT void
        dafka_producer (zsock_t *pipe, void *args);
    
    //
    DAFKA_EXPORT const char *
        dafka_producer_address (zactor_t *self);
    
    //  Self test of this class.
    DAFKA_EXPORT void
        dafka_producer_test (bool verbose);
    
```
Please add '@interface' section in './../src/dafka_producer.c'.

This is the class self test code:

```c
    //  Simple create/destroy test
    zconfig_t *config = zconfig_new ("root", NULL);
    zconfig_put (config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/sub_address","inproc://producer-tower-sub");
    zconfig_put (config, "beacon/pub_address","inproc://producer-tower-pub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address","inproc://producer-tower-sub");
    zconfig_put (config, "tower/pub_address","inproc://producer-tower-pub");
    zconfig_put (config, "producer/verbose", verbose ? "1" : "0");
    zconfig_put (config, "store/verbose", verbose ? "1" : "0");
    zconfig_put (config, "store/db", SELFTEST_DIR_RW "/storedb");
    
    zactor_t *tower = zactor_new (dafka_tower_actor, config);
    
    dafka_producer_args_t args = {"dummy", config};
    zactor_t *producer = zactor_new (dafka_producer, &args);
    assert (producer);
    
    zactor_destroy (&producer);
    zactor_destroy (&tower);
    zconfig_destroy (&config);
```

#### dafka_store - no title found

dafka_store -

Please add '@discuss' section in './../src/dafka_store.c'.

This is the class interface:

```h
    //  Create new dafka_store actor instance.
    //  @TODO: Describe the purpose of this actor!
    //
    //      zactor_t *dafka_store = zactor_new (dafka_store, NULL);
    //
    //  Destroy dafka_store instance.
    //
    //      zactor_destroy (&dafka_store);
    //
    //  Start dafka_store actor.
    //
    //      zstr_sendx (dafka_store, "START", NULL);
    //
    //  Stop dafka_store actor.
    //
    //      zstr_sendx (dafka_store, "STOP", NULL);
    //
    //  This is the dafka_store constructor as a zactor_fn;
    DAFKA_EXPORT void
        dafka_store_actor (zsock_t *pipe, void *args);
    
    //  Self test of this actor
    DAFKA_EXPORT void
        dafka_store_test (bool verbose);
```
Please add '@interface' section in './../src/dafka_store.c'.

This is the class self test code:

```c
    // ----------------------------------------------------
    //  Cleanup old test artifacts
    // ----------------------------------------------------
    if (zsys_file_exists (SELFTEST_DIR_RW "/storedb")) {
        zdir_t *store_dir = zdir_new (SELFTEST_DIR_RW "/storedb", NULL);
        zdir_remove (store_dir, true);
        zdir_destroy (&store_dir);
    }
    
    //  Simple create/destroy test
    zconfig_t *config = zconfig_new ("root", NULL);
    zconfig_put (config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/sub_address", "inproc://store-tower-sub");
    zconfig_put (config, "beacon/pub_address", "inproc://store-tower-pub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address", "inproc://store-tower-sub");
    zconfig_put (config, "tower/pub_address", "inproc://store-tower-pub");
    zconfig_put (config, "store/verbose", verbose ? "1" : "0");
    zconfig_put (config, "consumer/verbose", verbose ? "1" : "0");
    zconfig_put (config, "producer/verbose", verbose ? "1" : "0");
    zconfig_put (config, "store/db", SELFTEST_DIR_RW "/storedb");
    zconfig_put (config, "consumer/offset/reset", "earliest");
    
    // Creating the store
    zactor_t *tower = zactor_new (dafka_tower_actor, config);
    
    // Creating the publisher
    dafka_producer_args_t args = {"TEST", config};
    zactor_t *producer = zactor_new (dafka_producer, &args);
    
    // Producing before the store is alive, in order to test fetching between producer and store
    dafka_producer_msg_t *p_msg = dafka_producer_msg_new ();
    dafka_producer_msg_set_content_str (p_msg, "1");
    dafka_producer_msg_send (p_msg, producer);
    
    dafka_producer_msg_set_content_str (p_msg, "2");
    dafka_producer_msg_send (p_msg, producer);
    
    // Starting the store
    zactor_t *store = zactor_new (dafka_store_actor, config);
    zclock_sleep (100);
    
    // Producing another message
    dafka_producer_msg_set_content_str (p_msg, "3");
    dafka_producer_msg_send (p_msg, producer);
    zclock_sleep (100);
    
    // Killing the producer, to make sure the HEADs are coming from the store
    zactor_destroy (&producer);
    
    // Starting a consumer and check that consumer recv all 3 messages
    zactor_t *consumer = zactor_new (dafka_consumer, config);
    dafka_consumer_subscribe (consumer, "TEST");
    
    dafka_consumer_msg_t *c_msg = dafka_consumer_msg_new ();
    dafka_consumer_msg_recv (c_msg, consumer);
    assert (dafka_consumer_msg_streq (c_msg, "1"));
    
    dafka_consumer_msg_recv (c_msg, consumer);
    assert (dafka_consumer_msg_streq (c_msg, "2"));
    
    dafka_consumer_msg_recv (c_msg, consumer);
    assert (dafka_consumer_msg_streq (c_msg, "3"));
    
    dafka_consumer_msg_destroy (&c_msg);
    zactor_destroy (&consumer);
    dafka_producer_msg_destroy (&p_msg);
    zactor_destroy (&store);
    zactor_destroy (&tower);
    zconfig_destroy (&config);
    
    // ----------------------------------------------------
    //  Cleanup test artifacts
    // ----------------------------------------------------
    zdir_t *store_dir = zdir_new (SELFTEST_DIR_RW "/storedb", NULL);
    zdir_remove (store_dir, true);
    zdir_destroy (&store_dir);
```

#### dafka_tower - no title found

dafka_tower -

Please add '@discuss' section in './../src/dafka_tower.c'.

This is the class interface:

```h
    //  Create new dafka_tower actor instance.
    //  @TODO: Describe the purpose of this actor!
    //
    //      zactor_t *dafka_tower = zactor_new (dafka_tower, NULL);
    //
    //  Destroy dafka_tower instance.
    //
    //      zactor_destroy (&dafka_tower);
    //
    //  Start dafka_tower actor.
    //
    //      zstr_sendx (dafka_tower, "START", NULL);
    //
    //  Stop dafka_tower actor.
    //
    //      zstr_sendx (dafka_tower, "STOP", NULL);
    //
    //  This is the dafka_tower constructor as a zactor_fn;
    DAFKA_EXPORT void
        dafka_tower_actor (zsock_t *pipe, void *args);
    
    //  Self test of this actor
    DAFKA_EXPORT void
        dafka_tower_test (bool verbose);
```
Please add '@interface' section in './../src/dafka_tower.c'.

This is the class self test code:

```c
    //  Simple create/destroy test
    /*
    zactor_t *dafka_tower = zactor_new (dafka_tower_actor, NULL);
    assert (dafka_tower);
    
    zactor_destroy (&dafka_tower);
    */
```


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

_This documentation was generated from dafka/README.txt using [Gitdown](https://github.com/zeromq/gitdown)_
