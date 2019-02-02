
[![GitHub release](https://img.shields.io/github/release/zeromq/dafka.svg)](https://github.com/zeromq/dafka/releases)
<a target="_blank" href="http://webchat.freenode.net?channels=%23zeromq&uio=d4"><img src="https://cloud.githubusercontent.com/assets/493242/14886493/5c660ea2-0d51-11e6-8249-502e6c71e9f2.png" height = "20" /></a>
[![license](https://img.shields.io/github/license/zeromq/dafka.svg)](https://github.com/zeromq/dafka/blob/master/LICENSE)

# Dafka - Distributed dezentralized Kafka replacement

[![Build Status](https://travis-ci.org/zeromq/dafka.png?branch=master)](https://travis-ci.org/zeromq/dafka)

## Contents


**[Overview](#overview)**

**[Scope and Goals](#scope-and-goals)**

**[Ownership and License](#ownership-and-license)**

**[Using Dafka](#using-dafka)**

**[Building and Installing](#building-and-installing)**

**[Linking with an Application](#linking-with-an-application)**

**[API v1 Summary](#api-v1-summary)**
*  [dafka_publisher - no title found](#dafka_publisher---no-title-found)
*  [dafka_store - no title found](#dafka_store---no-title-found)
*  [dafka_subscriber - no title found](#dafka_subscriber---no-title-found)
*  [dafka_tower - no title found](#dafka_tower---no-title-found)

**[Documentation](#documentation)**

**[Development](#development)**

**[Hints to Contributors](#hints-to-contributors)**

**[Code Generation](#code-generation)**

**[This Document](#this-document)**

## Overview

### Scope and Goals

TODO

### Ownership and License

The contributors are listed in AUTHORS. This project uses the MPL v2 license, see LICENSE.

Dafka uses the [C4.1 (Collective Code Construction Contract)](http://rfc.zeromq.org/spec:22) process for contributions.

Dafka uses the [CLASS (C Language Style for Scalabilty)](http://rfc.zeromq.org/spec:21) guide for code style.

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

#### dafka_publisher - no title found

dafka_publisher -

TODO:
    - Store send messages until an ACK has been received

This is the class interface:

```h
    //  Create new dafka_publisher actor instance.
    //  @TODO: Describe the purpose of this actor!
    //
    //      zactor_t *dafka_publisher = zactor_new (dafka_publisher, NULL);
    //
    //  Destroy dafka_publisher instance.
    //
    //      zactor_destroy (&dafka_publisher);
    //
    //  Start dafka_publisher actor.
    //
    //      zstr_sendx (dafka_publisher, "START", NULL);
    //
    //  Stop dafka_publisher actor.
    //
    //      zstr_sendx (dafka_publisher, "STOP", NULL);
    //
    //  This is the dafka_publisher constructor as a zactor_fn;
    DAFKA_EXPORT void
        dafka_publisher_actor (zsock_t *pipe, void *args);
    
    //  Publish content
    DAFKA_EXPORT int
        dafka_publisher_publish (zactor_t *self, zframe_t **content);
    
    //  Get the address the publisher
    DAFKA_EXPORT const char *
        dafka_publisher_address (zactor_t *self);
    
    //  Self test of this actor
    DAFKA_EXPORT void
        dafka_publisher_test (bool verbose);
```
Please add '@interface' section in './../src/dafka_publisher.c'.

This is the class self test code:

```c
    //  Simple create/destroy test
    zconfig_t *config = zconfig_new ("root", NULL);
    zconfig_put (config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/sub_address","inproc://tower-sub");
    zconfig_put (config, "beacon/pub_address","inproc://tower-pub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address","inproc://tower-sub");
    zconfig_put (config, "tower/pub_address","inproc://tower-pub");
    zconfig_put (config, "producer/verbose", verbose ? "1" : "0");
    
    zactor_t *tower = zactor_new (dafka_tower_actor, config);
    
    dafka_publisher_args_t args = {"dummy", config};
    zactor_t *dafka_publisher = zactor_new (dafka_publisher_actor, &args);
    assert (dafka_publisher);
    
    zactor_destroy (&dafka_publisher);
    zactor_destroy (&tower);
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
    zconfig_put (config, "beacon/sub_address","inproc://tower-sub");
    zconfig_put (config, "beacon/pub_address","inproc://tower-pub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address","inproc://tower-sub");
    zconfig_put (config, "tower/pub_address","inproc://tower-pub");
    zconfig_put (config, "store/verbose", verbose ? "1" : "0");
    
    //    char *consumer_address = "SUB";
    
    //    // Creating the publisher
    //    dafka_publisher_t *pub = dafka_publisher_new ("TEST", config);
    
    //    // Creating the consumer pub socket
    //    zsock_t *consumer_pub = zsock_new_pub (consumer_endpoint);
    
    // Creating the store
    zactor_t *tower = zactor_new (dafka_tower_actor, config);
    zactor_t *store = zactor_new (dafka_store_actor, config);
    
    //    // Creating the consumer sub socker and subscribe
    //    zsock_t *consumer_sub = zsock_new_sub (store_endpoint, NULL);
    //    dafka_proto_subscribe (consumer_sub, DAFKA_PROTO_DIRECT, consumer_address);
    //
    //    // Publish message, store should receive and store
    //    zframe_t *content = zframe_new ("HELLO", 5);
    //    dafka_publisher_publish (pub, &content);
    //
    //    content = zframe_new ("WORLD", 5);
    //    dafka_publisher_publish (pub, &content);
    //
    //    usleep (100);
    //
    //    // Consumer ask for a message
    //    dafka_proto_t *msg = dafka_proto_new ();
    //    dafka_proto_set_topic (msg, dafka_publisher_address(pub));
    //    dafka_proto_set_subject (msg, "TEST");
    //    dafka_proto_set_sequence (msg, 0);
    //    dafka_proto_set_count (msg, 2);
    //    dafka_proto_set_address (msg, consumer_address);
    //    dafka_proto_set_id (msg, DAFKA_PROTO_FETCH);
    //    dafka_proto_send (msg, consumer_pub);
    //
    //    // Consumer wait for a response from store
    //    int rc = dafka_proto_recv (msg, consumer_sub);
    //    assert (rc == 0);
    //    assert (dafka_proto_id (msg) == DAFKA_PROTO_DIRECT);
    //    assert (streq (dafka_proto_topic (msg), consumer_address));
    //    assert (streq (dafka_proto_subject (msg), "TEST"));
    //    assert (dafka_proto_sequence (msg) == 0);
    //    assert (zframe_streq (dafka_proto_content (msg), "HELLO"));
    //
    //    // Receiving the second message
    //    dafka_proto_recv (msg, consumer_sub);
    //    assert (rc == 0);
    //    assert (dafka_proto_id (msg) == DAFKA_PROTO_DIRECT);
    //    assert (streq (dafka_proto_topic (msg), consumer_address));
    //    assert (streq (dafka_proto_subject (msg), "TEST"));
    //    assert (dafka_proto_sequence (msg) == 1);
    //    assert (zframe_streq (dafka_proto_content (msg), "WORLD"));
    //
    
    //    dafka_proto_destroy (&msg);
    //    zsock_destroy (&consumer_sub);
    zactor_destroy (&store);
    zactor_destroy (&tower);
    //    zsock_destroy (&consumer_pub);
    //    dafka_publisher_destroy (&pub);
```

#### dafka_subscriber - no title found

dafka_subscriber -

TODO:
    - Option start consuming from beginning or latest (config)
    - Add parameter in console-consumer

This is the class interface:

```h
    //  Create new dafka_subscriber actor instance.
    //  @TODO: Describe the purpose of this actor!
    //
    //      zactor_t *dafka_subscriber = zactor_new (dafka_subscriber, "publisher-address");
    //
    //  Destroy dafka_subscriber instance.
    //
    //      zactor_destroy (&dafka_subscriber);
    //
    //  This is the dafka_subscriber constructor as a zactor_fn;
    DAFKA_EXPORT void
        dafka_subscriber_actor (zsock_t *pipe, void *args);
    
    //  Self test of this actor
    DAFKA_EXPORT void
        dafka_subscriber_test (bool verbose);
```
Please add '@interface' section in './../src/dafka_subscriber.c'.

This is the class self test code:

```c
    zconfig_t *config = zconfig_new ("root", NULL);
    zconfig_put (config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/sub_address", "inproc://tower-sub");
    zconfig_put (config, "beacon/pub_address", "inproc://tower-pub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address", "inproc://tower-sub");
    zconfig_put (config, "tower/pub_address", "inproc://tower-pub");
    zconfig_put (config, "consumer/verbose", verbose ? "1" : "0");
    zconfig_put (config, "producer/verbose", verbose ? "1" : "0");
    zconfig_put (config, "store/verbose", verbose ? "1" : "0");
    
    zactor_t *tower = zactor_new (dafka_tower_actor, config);
    
    dafka_publisher_args_t pub_args = {"hello", config};
    zactor_t *pub =  zactor_new (dafka_publisher_actor, &pub_args);
    assert (pub);
    
    zactor_t *store = zactor_new (dafka_store_actor, config);
    assert (store);
    
    zactor_t *sub = zactor_new (dafka_subscriber_actor, config);
    assert (sub);
    zclock_sleep (1000);
    
    zframe_t *content = zframe_new ("HELLO MATE", 10);
    int rc = dafka_publisher_publish (pub, &content);
    assert (rc == 0);
    sleep (1);  // Make sure message is published before subscriber subscribes
    
    rc = zsock_send (sub, "ss", "SUBSCRIBE", "hello");
    assert (rc == 0);
    zclock_sleep (1000);  // Make sure subscription is active before sending the next message
    
    // This message is discarded but triggers a FETCH from the store
    content = zframe_new ("HELLO ATEM", 10);
    rc = dafka_publisher_publish (pub, &content);
    assert (rc == 0);
    sleep (1);  // Make sure the first two messages have been received from the store and the subscriber is now up to date
    
    content = zframe_new ("HELLO TEMA", 10);
    rc = dafka_publisher_publish (pub, &content);
    assert (rc == 0);
    
    char *topic;
    char *address;
    char *content_str;
    
    // Receive the first message from the STORE
    zsock_brecv (sub, "ssf", &topic, &address, &content);
    content_str = zframe_strdup (content);
    assert (streq (topic, "hello"));
    assert (streq (content_str, "HELLO MATE"));
    zstr_free (&content_str);
    zframe_destroy (&content);
    
    // Receive the second message from the STORE as the original has been discarded
    zsock_brecv (sub, "ssf", &topic, &address, &content);
    content_str = zframe_strdup (content);
    assert (streq (topic, "hello"));
    assert (streq (content_str, "HELLO ATEM"));
    zstr_free (&content_str);
    zframe_destroy (&content);
    
    // Receive the third message from the PUBLISHER
    zsock_brecv (sub, "ssf", &topic, &address, &content);
    content_str = zframe_strdup (content);
    assert (streq (topic, "hello"));
    assert (streq (content_str, "HELLO TEMA"));
    zstr_free (&content_str);
    zframe_destroy (&content);
    
    zactor_destroy (&pub);
    zactor_destroy (&store);
    zactor_destroy (&sub);
    zactor_destroy (&tower);
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
