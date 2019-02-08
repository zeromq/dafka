
[![GitHub release](https://img.shields.io/github/release/zeromq/dafka.svg)](https://github.com/zeromq/dafka/releases)
<a target="_blank" href="http://webchat.freenode.net?channels=%23zeromq&uio=d4"><img src="https://cloud.githubusercontent.com/assets/493242/14886493/5c660ea2-0d51-11e6-8249-502e6c71e9f2.png" height = "20" /></a>
[![license](https://img.shields.io/badge/license-MPLV2.0-blue.svg)](https://github.com/zeromq/dafka/blob/master/LICENSE)

# Dafka - Decentralized Distributed Streaming Platform

[![Build Status](https://travis-ci.org/zeromq/dafka.png?branch=master)](https://travis-ci.org/zeromq/dafka)

## Contents


**[Overview](#overview)**

**[Scope and Goals](#scope-and-goals)**

**[Topics and Partitions](#topics-and-partitions)**

**[Distribution](#distribution)**

**[Producer](#producer)**

**[Consumer](#consumer)**

**[Guarantees](#guarantees)**

**[Design](#design)**

**[Producing and Storing](#producing-and-storing)**

**[Missed messages](#missed-messages)**

**[Dead producer](#dead-producer)**

**[Implementation](#implementation)**

**[Ownership and License](#ownership-and-license)**

**[Using Dafka](#using-dafka)**

**[Building and Installing](#building-and-installing)**

**[Linking with an Application](#linking-with-an-application)**

**[API v1 Summary](#api-v1-summary)**
*  [dafka_consumer - no title found](#dafka_consumer---no-title-found)
*  [dafka_producer - no title found](#dafka_producer---no-title-found)
*  [dafka_store - no title found](#dafka_store---no-title-found)
*  [dafka_tower - no title found](#dafka_tower---no-title-found)

**[Documentation](#documentation)**

**[Development](#development)**

**[Hints to Contributors](#hints-to-contributors)**

**[Code Generation](#code-generation)**

**[This Document](#this-document)**

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
neither the Dafka Cluster nor the Producers keep track of the consumers offset.
This design allows Consumer to either reset their offset to an older offset and
re-read records or set their offset to a newer offset and skip ahead.

In that way consumer have no influence on the cluster, the producer and other
consumers. They simply can come and go as they please.

### Distribution

Partition are distributed to the Dafka Cluster which consists of Dafka Stores.
Each partition is replicated to each store for fault tolerance.

### Producer

Producers publish records to a topic. Each producer creates its own partition
that only it publishes to. Records are send directly to stores and consumers.
When a producer goes offline the consumers can retrieve to already send records
from the stores.

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

Therefore it would have to have higher throughput and lower latency.

### Producing and Storing

<center>
<img src="https://github.com/zeromq/dafka/raw/master/images/README_3.png" alt="3">
</center>

<center>
<img src="https://github.com/zeromq/dafka/raw/master/images/README_4.png" alt="4">
</center>

### Missed messages

<center>
<img src="https://github.com/zeromq/dafka/raw/master/images/README_5.png" alt="5">
</center>

<center>
<img src="https://github.com/zeromq/dafka/raw/master/images/README_6.png" alt="6">
</center>

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

#### dafka_consumer - no title found

dafka_consumer -

TODO:
  - Send earliest message when a store connects

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
    // ----------------------------------------------------
    // Test with consumer.offset.reset = earliest
    // ----------------------------------------------------
    zconfig_t *config = zconfig_new ("root", NULL);
    zconfig_put (config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/sub_address", "inproc://consumer-tower-sub");
    zconfig_put (config, "beacon/pub_address", "inproc://consumer-tower-pub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address", "inproc://consumer-tower-sub");
    zconfig_put (config, "tower/pub_address", "inproc://consumer-tower-pub");
    zconfig_put (config, "consumer/verbose", verbose ? "1" : "0");
    zconfig_put (config, "consumer/offset/reset", "earliest");
    zconfig_put (config, "producer/verbose", verbose ? "1" : "0");
    zconfig_put (config, "store/verbose", verbose ? "1" : "0");
    zconfig_put (config, "store/db", SELFTEST_DIR_RW "/storedb");
    
    zactor_t *tower = zactor_new (dafka_tower_actor, config);
    
    dafka_producer_args_t pub_args = { "hello", config };
    zactor_t *producer =  zactor_new (dafka_producer, &pub_args);
    assert (producer);
    
    zactor_t *store = zactor_new (dafka_store_actor, config);
    assert (store);
    
    zactor_t *consumer = zactor_new (dafka_consumer, config);
    assert (consumer);
    zclock_sleep (100);
    
    dafka_producer_msg_t *p_msg = dafka_producer_msg_new ();
    dafka_producer_msg_set_content_str (p_msg , "HELLO MATE");
    int rc = dafka_producer_msg_send (p_msg, producer);
    assert (rc == 0);
    zclock_sleep (100);  // Make sure message is published before consumer subscribes
    
    rc = dafka_consumer_subscribe (consumer, "hello");
    assert (rc == 0);
    zclock_sleep (250);  // Make sure subscription is active before sending the next message
    
    // This message is discarded but triggers a FETCH from the store
    dafka_producer_msg_set_content_str (p_msg, "HELLO ATEM");
    rc = dafka_producer_msg_send (p_msg, producer);
    assert (rc == 0);
    zclock_sleep (100);  // Make sure the first two messages have been received from the store and the consumer is now up to date
    
    dafka_producer_msg_set_content_str (p_msg, "HELLO TEMA");
    rc = dafka_producer_msg_send (p_msg, producer);
    assert (rc == 0);
    
    // Receive the first message from the STORE
    dafka_consumer_msg_t *c_msg = dafka_consumer_msg_new ();
    dafka_consumer_msg_recv (c_msg, consumer);
    assert (streq (dafka_consumer_msg_subject (c_msg), "hello"));
    assert (dafka_consumer_msg_streq (c_msg, "HELLO MATE"));
    
    // Receive the second message from the STORE as the original has been discarded
    dafka_consumer_msg_recv (c_msg, consumer);
    assert (streq (dafka_consumer_msg_subject (c_msg), "hello"));
    assert (dafka_consumer_msg_streq (c_msg, "HELLO ATEM"));
    
    // Receive the third message from the PUBLISHER
    dafka_consumer_msg_recv (c_msg, consumer);
    assert (streq (dafka_consumer_msg_subject (c_msg), "hello"));
    assert (dafka_consumer_msg_streq (c_msg, "HELLO TEMA"));
    
    dafka_producer_msg_destroy (&p_msg);
    dafka_consumer_msg_destroy (&c_msg);
    zactor_destroy (&producer);
    zactor_destroy (&store);
    zactor_destroy (&consumer);
    
    // ----------------------------------------------------
    // Test with consumer.offset.reset = latest
    // ----------------------------------------------------
    zconfig_put (config, "consumer/offset/reset", "latest");
    
    producer =  zactor_new (dafka_producer, &pub_args);
    assert (producer);
    
    consumer = zactor_new (dafka_consumer, config);
    assert (consumer);
    zclock_sleep (100);
    
    //  This message is missed by the consumer and later ignored because the
    //  offset reset is set to latest.
    p_msg = dafka_producer_msg_new ();
    dafka_producer_msg_set_content_str (p_msg , "HELLO MATE");
    rc = dafka_producer_msg_send (p_msg, producer);
    assert (rc == 0);
    zclock_sleep (100);  // Make sure message is published before consumer subscribes
    
    rc = dafka_consumer_subscribe (consumer, "hello");
    assert (rc == 0);
    zclock_sleep (250);  // Make sure subscription is active before sending the next message
    
    dafka_producer_msg_set_content_str (p_msg , "HELLO ATEM");
    rc = dafka_producer_msg_send (p_msg, producer);
    assert (rc == 0);
    
    // Receive the second message from the PRODUCER
    c_msg = dafka_consumer_msg_new ();
    dafka_consumer_msg_recv (c_msg, consumer);
    assert (streq (dafka_consumer_msg_subject (c_msg), "hello"));
    assert (dafka_consumer_msg_streq (c_msg, "HELLO ATEM"));
    
    dafka_producer_msg_destroy (&p_msg);
    dafka_consumer_msg_destroy (&c_msg);
    zactor_destroy (&tower);
    zactor_destroy (&producer);
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
    zclock_sleep (2000);
    
    // Producing another message
    dafka_producer_msg_set_content_str (p_msg, "3");
    dafka_producer_msg_send (p_msg, producer);
    
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
    zactor_destroy (&producer);
    zconfig_destroy (&config);
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

This document is originally at README.txt and is built using [gitdown](http://github.com/imatix/gitdown).

_This documentation was generated from dafka/README.txt using [Gitdown](https://github.com/zeromq/gitdown)_
