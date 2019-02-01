/*  =========================================================================
    dafka_store -

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

/*
@header
    dafka_store -
@discuss
@end
*/

#include "dafka_classes.h"

//  Structure of our actor

typedef struct {
    char subject [256];
    char address [256];
    uint64_t sequence;

    size_t hash;
} store_key_t;

struct _dafka_store_t {
    zsock_t *pipe;              //  Actor command pipe
    zpoller_t *poller;          //  Socket poller
    bool terminated;            //  Did caller ask us to quit?
    bool verbose;               //  Verbose logging enabled?

    dafka_proto_t *income_msg;
    dafka_proto_t *outgoing_msg;
    zsock_t *pub;
    zsock_t *sub;
    zactor_t *beacon;

    zhashx_t *store;
    char *address;
};

static void
store_key_init (store_key_t *self, const char *subject, const char *address, uint64_t sequence) {

    strcpy (self->subject, subject);
    strcpy (self->address, address);
    self->sequence = sequence;

    self->hash = 0;

    const char *pointer = (const char *) &self->subject;
    for (size_t i = 0; i < strlen (self->subject) ; i++) {
        self->hash = 33 * self->hash ^ *pointer++;
    }

    pointer = (const char *) &self->address;
    for (size_t i = 0; i < strlen (self->address) ; i++) {
        self->hash = 33 * self->hash ^ *pointer++;
    }

    pointer = (const char *) &self->sequence;
    for (size_t i = 0; i < sizeof (uint64_t) ; i++) {
        self->hash = 33 * self->hash ^ *pointer++;
    }
}

static store_key_t *
store_key_dup (store_key_t *self) {
    store_key_t *copy = zmalloc (sizeof (store_key_t));

    *copy = *self;

    return copy;
}

static size_t
store_hash (const store_key_t *self) {
    return self->hash;
}

static void
store_key_destroy (store_key_t **self_p) {
    assert (self_p);
    store_key_t *self = *self_p;

    if (self) {
        free (self);
    }

    *self_p = NULL;
}

static int
store_key_cmp (const store_key_t *item1, const store_key_t *item2) {
    int rc = strcmp (item1->subject, item2->subject);
    if (rc != 0)
        return rc;

    rc = strcmp (item1->address, item2->address);
    if (rc != 0)
        return rc;

    return (int)(item1->sequence - item2->sequence);
}

static void*
take_ownership_dup (const void* self) {
    return (void*) self;
}

//  --------------------------------------------------------------------------
//  Create a new dafka_store instance

static dafka_store_t *
dafka_store_new (zsock_t *pipe, zconfig_t *config)
{
    dafka_store_t *self = (dafka_store_t *) zmalloc (sizeof (dafka_store_t));
    assert (self);

    self->pipe = pipe;
    self->terminated = false;

    zuuid_t *uuid = zuuid_new ();
    self->address = strdup (zuuid_str (uuid));
    zuuid_destroy (&uuid);

    self->income_msg = dafka_proto_new ();
    self->outgoing_msg = dafka_proto_new ();
    self->pub = zsock_new_pub (NULL);
    int port = zsock_bind (self->pub, "tcp://*:*");
    assert (port != -1);
    self->sub = zsock_new_sub (NULL, NULL);
    dafka_proto_subscribe (self->sub, DAFKA_PROTO_MSG, "");
    dafka_proto_subscribe (self->sub, DAFKA_PROTO_FETCH, "");

    self->beacon = zactor_new (dafka_beacon_actor, config);
    zsock_send (self->beacon, "ssi", "START", self->address, port);
    assert (zsock_wait (self->beacon) == 0);

    self->store = zhashx_new ();
    zhashx_set_destructor (self->store, (zhashx_destructor_fn *) zframe_destroy);
    zhashx_set_duplicator (self->store, take_ownership_dup);
    zhashx_set_key_destructor (self->store, (zhashx_destructor_fn *) store_key_destroy);
    zhashx_set_key_duplicator (self->store, (zhashx_duplicator_fn *) store_key_dup);
    zhashx_set_key_hasher (self->store, (zhashx_hash_fn *) store_hash);
    zhashx_set_key_comparator (self->store, (zhashx_comparator_fn *) store_key_cmp);

    self->poller = zpoller_new (self->pipe, self->sub, self->beacon, NULL);

    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_store instance

static void
dafka_store_destroy (dafka_store_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_store_t *self = *self_p;

        zstr_free (&self->address);
        zsock_destroy (&self->sub);
        zsock_destroy (&self->pub);
        dafka_proto_destroy (&self->income_msg);
        dafka_proto_destroy (&self->outgoing_msg);
        zhashx_destroy (&self->store);
        zactor_destroy (&self->beacon);

        //  Free object itself
        zpoller_destroy (&self->poller);
        free (self);
        *self_p = NULL;
    }
}

//  Here we handle incoming message from the node

static void
dafka_store_recv_api (dafka_store_t *self)
{
    //  Get the whole message of the pipe in one go
    zmsg_t *request = zmsg_recv (self->pipe);
    if (!request)
       return;        //  Interrupted

    char *command = zmsg_popstr (request);

    if (streq (command, "VERBOSE"))
        self->verbose = true;
    else
    if (streq (command, "$TERM"))
        //  The $TERM command is send by zactor_destroy() method
        self->terminated = true;
    else {
        zsys_error ("invalid command '%s'", command);
        assert (false);
    }
    zstr_free (&command);
    zmsg_destroy (&request);
}

// Handle messages from network

static void
dafka_store_recv_sub (dafka_store_t *self) {
    int rc = dafka_proto_recv (self->income_msg, self->sub);
    if (rc == -1) //  Interrupted
        return;

    store_key_t key = {0};

    switch (dafka_proto_id (self->income_msg)) {
        case DAFKA_PROTO_MSG: {
            const char *subject = dafka_proto_topic (self->income_msg);
            const char *address = dafka_proto_address (self->income_msg);
            uint64_t sequence = dafka_proto_sequence (self->income_msg);

            store_key_init (&key, subject, address, sequence);
            zhashx_insert (self->store, &key, dafka_proto_get_content (self->income_msg));

            zsys_info ("Store: storing a message. Subject: %s, Partition: %s, Seq: %u",
                    subject, address, sequence);

            // Sending an ack to the producer
            dafka_proto_set_id (self->outgoing_msg,  DAFKA_PROTO_ACK);
            dafka_proto_set_topic (self->outgoing_msg, address);
            dafka_proto_set_subject (self->outgoing_msg, subject);
            dafka_proto_set_sequence (self->outgoing_msg, sequence);
            dafka_proto_send (self->outgoing_msg, self->pub);

            break;
        }
        case DAFKA_PROTO_FETCH: {
            const char *subject = dafka_proto_subject (self->income_msg);
            const char *address = dafka_proto_topic (self->income_msg);
            uint64_t sequence = dafka_proto_sequence (self->income_msg);
            uint32_t count = dafka_proto_count (self->income_msg);

            dafka_proto_set_topic (self->outgoing_msg, dafka_proto_address (self->income_msg));
            dafka_proto_set_subject (self->outgoing_msg, subject);
            dafka_proto_set_address (self->outgoing_msg, address);
            dafka_proto_set_id (self->outgoing_msg, DAFKA_PROTO_DIRECT);

            for (uint32_t i = 0; i < count; i++) {
                store_key_init (&key, subject, address, sequence + i);

                zframe_t *frame = zhashx_lookup (self->store, &key);

                if (frame) {
                    zsys_info ("Store: found answer for subscriber. Subject: %s, Partition: %s, Seq: %u",
                               subject, address, sequence + i);

                    // TODO: add zframe_copy that will make a zmq_msg_copy instead of full frame copy
                    // We can also use the zframe_frommem
                    frame = zframe_dup (frame);

                    // The answer topic is the asker address
                    dafka_proto_set_sequence (self->outgoing_msg, sequence + i);
                    dafka_proto_set_content (self->outgoing_msg, &frame);
                    dafka_proto_send (self->outgoing_msg, self->pub);
                } else {
                    zsys_info ("Store: no answer for subscriber. Subject: %s, Partition: %s, Seq: %u",
                               subject, address, sequence);
                    break;
                }
            }

            break;
        }

        default:
            return;
    }
}

//  --------------------------------------------------------------------------
//  This is the actor which runs in its own thread.

void
dafka_store_actor (zsock_t *pipe, void *arg)
{
    dafka_store_t * self = dafka_store_new (pipe, arg);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (self->pipe, 0);

    zsys_info ("Store: running...");

    while (!self->terminated) {
        void *which = (zsock_t *) zpoller_wait (self->poller, -1);
        if (which == self->pipe)
            dafka_store_recv_api (self);
        if (which == self->sub)
            dafka_store_recv_sub (self);
        if (which == self->beacon)
            dafka_beacon_recv (self->beacon, self->sub);
    }

    zsys_info ("Store: stopped");

    dafka_store_destroy (&self);
}

//  --------------------------------------------------------------------------
//  Self test of this actor.

// If your selftest reads SCMed fixture data, please keep it in
// src/selftest-ro; if your test creates filesystem objects, please
// do so under src/selftest-rw.
// The following pattern is suggested for C selftest code:
//    char *filename = NULL;
//    filename = zsys_sprintf ("%s/%s", SELFTEST_DIR_RO, "mytemplate.file");
//    assert (filename);
//    ... use the "filename" for I/O ...
//    zstr_free (&filename);
// This way the same "filename" variable can be reused for many subtests.
#define SELFTEST_DIR_RO "src/selftest-ro"
#define SELFTEST_DIR_RW "src/selftest-rw"

void
dafka_store_test (bool verbose)
{
    printf (" * dafka_store: ");
    //  @selftest
    //  Simple create/destroy test
    zconfig_t *config = zconfig_new ("root", NULL);
    zconfig_put (config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/sub_address","inproc://tower-sub");
    zconfig_put (config, "beacon/pub_address","inproc://tower-sub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address","inproc://tower-sub");
    zconfig_put (config, "tower/pub_address","inproc://tower-sub");

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
    //  @end

    printf ("OK\n");
}
