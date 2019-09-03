/*  =========================================================================
    dafka_publisher -

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
    dafka_publisher -
@discuss
@end
*/

#include "dafka_classes.h"

//  Structure of our actor

struct _dafka_producer_t {
    //  Actor properties
    zsock_t *pipe;              //  Actor command pipe
    zloop_t *loop;              //  Event loop
    bool verbose;               //  Verbose logging enabled?

    //  Class properties
    zsock_t *socket;            // Socket to publish messages to
    zsock_t *producer_sub;      // Socket to subscribe to messages
    dafka_proto_t *msg;         // Reusable MSG message to publish
    dafka_proto_t *head_msg;    // Reusable HEAD message to publish
    dafka_proto_t *sub_msg;     // Reusable ACK message to receive
    zhashx_t *message_cache;    // Messages are keept in the cache until the ACK is received
    uint64_t last_acked_sequence; // Last sequence no that has been acked by a store

    size_t head_interval;

    zactor_t *beacon;
};

static int
s_recv_api (zloop_t *loop, zsock_t *pipe, void *arg);

static int
s_recv_socket (zloop_t *loop, zsock_t *pipe, void *arg);

static int
s_recv_sub (zloop_t *loop, zsock_t *pipe, void *arg);

static int
s_recv_beacon (zloop_t *loop, zsock_t *pipe, void *arg);

static int
s_send_head (zloop_t *loop, int timer_id, void *arg);

//  --------------------------------------------------------------------------
//  Create a new dafka_producer instance

static dafka_producer_t *
dafka_producer_new (zsock_t *pipe, dafka_producer_args_t *args)
{
    dafka_producer_t *self = (dafka_producer_t *) zmalloc (sizeof (dafka_producer_t));
    assert (self);
    assert (args);

    //  Initialize actor properties
    self->pipe = pipe;
    self->loop = zloop_new ();

    //  Initialize class properties
    if (atoi (zconfig_get (args->config, "producer/verbose", "0")))
        self->verbose = true;

    self->head_interval = atoi (zconfig_get (args->config, "producer/head_interval", "1000"));

    self->socket = zsock_new_pub (NULL);
    self->producer_sub = zsock_new_sub (NULL, NULL);
    int port = zsock_bind (self->socket, "tcp://*:*");
    assert (self->socket);

    self->msg = dafka_proto_new ();
    dafka_proto_set_id (self->msg, DAFKA_PROTO_RECORD);
    dafka_proto_set_topic (self->msg, args->topic);
    dafka_proto_set_subject (self->msg, args->topic);
    zuuid_t *address = zuuid_new ();
    dafka_proto_set_address (self->msg, zuuid_str (address));
    dafka_proto_set_sequence (self->msg, -1);
    self->last_acked_sequence = -1;

    self->head_msg = dafka_proto_new ();
    dafka_proto_set_id (self->head_msg, DAFKA_PROTO_HEAD);
    dafka_proto_set_topic (self->head_msg, args->topic);
    dafka_proto_set_subject (self->head_msg, args->topic);
    dafka_proto_set_address (self->head_msg, zuuid_str (address));

    self->sub_msg = dafka_proto_new ();

    dafka_beacon_args_t beacon_args = {"Producer", args->config};
    self->beacon = zactor_new (dafka_beacon_actor, &beacon_args);
    zsock_send (self->beacon, "ssi", "START", zuuid_str (address), port);
    zuuid_destroy (&address);

    self->message_cache = zhashx_new ();
    zhashx_set_key_destructor(self->message_cache, uint64_destroy);
    zhashx_set_key_duplicator (self->message_cache, uint64_dup);
    zhashx_set_destructor (self->message_cache, (zhashx_destructor_fn *) dafka_proto_destroy);
    zhashx_set_duplicator (self->message_cache, (zhashx_duplicator_fn *) dafka_proto_dup);
    zhashx_set_key_comparator (self->message_cache, uint64_cmp);
    zhashx_set_key_hasher (self->message_cache, uint64_hash);

    zloop_reader (self->loop, self->socket, s_recv_socket, self);
    zloop_reader (self->loop, self->producer_sub, s_recv_sub, self);
    zloop_reader (self->loop, self->pipe, s_recv_api, self);
    zloop_reader (self->loop, zactor_sock (self->beacon), s_recv_beacon, self);

    dafka_proto_subscribe (self->producer_sub, DAFKA_PROTO_ACK, dafka_proto_address (self->msg));
    dafka_proto_subscribe (self->producer_sub, DAFKA_PROTO_FETCH, dafka_proto_address (self->msg));
    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_producer instance

static void
dafka_producer_destroy (dafka_producer_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_producer_t *self = *self_p;

        //  Free class properties
        zloop_destroy (&self->loop);
        zsock_destroy (&self->socket);
        zsock_destroy (&self->producer_sub);
        dafka_proto_destroy (&self->msg);
        dafka_proto_destroy (&self->head_msg);
        dafka_proto_destroy (&self->sub_msg);
        zactor_destroy (&self->beacon);
        zhashx_destroy (&self->message_cache);

        //  Free actor properties
        free (self);
        *self_p = NULL;
    }
}

//  Publish content

static int
s_publish (dafka_producer_t *self, zframe_t *content)
{
    assert (self);
    assert (content);

    uint64_t sequence = dafka_proto_sequence (self->msg) + 1;
    dafka_proto_set_content (self->msg, &content);
    dafka_proto_set_sequence (self->msg, sequence);
    int rc = dafka_proto_send (self->msg, self->socket);

    if (self->verbose)
        zsys_debug ("Producer: Send MSG message with sequence %u", dafka_proto_sequence (self->msg));

    zhashx_insert (self->message_cache, &sequence, self->msg);

    // Starts the HEAD timer once the first message has been send
    if (sequence == 0)
        zloop_timer (self->loop, self->head_interval, 0, s_send_head, self);

    return rc;
}

//  Send a HEAD message

static int
s_send_head (zloop_t *loop, int timer_id, void *arg)
{
    assert (arg);
    dafka_producer_t *self = (dafka_producer_t  *) arg;
    uint64_t sequence = dafka_proto_sequence (self->msg);
    dafka_proto_set_sequence (self->head_msg, sequence);
    if (self->verbose)
        zsys_debug ("Producer: Send HEAD message with sequence %u", sequence);

    return dafka_proto_send (self->head_msg, self->socket);
}

//  Here we handle incoming message from the beacon

static int
s_recv_beacon (zloop_t *loop, zsock_t *pipe, void *arg)
{
    assert (loop);
    assert (pipe);
    assert (arg);
    dafka_producer_t *self = (dafka_producer_t  *) arg;

    dafka_beacon_recv (self->beacon, self->producer_sub, self->verbose, "Producer");
    return 0;
}


static int
s_recv_socket (zloop_t *loop, zsock_t *pipe, void *arg)
{
    // Nothing todo, PUB socket doesn't recv any messages
    // We only add it to zloop in order to process subscriptions

    return 0;
}


static int
s_recv_sub (zloop_t *loop, zsock_t *pipe, void *arg)
{
    assert (loop);
    assert (pipe);
    assert (arg);
    dafka_producer_t *self = (dafka_producer_t  *) arg;
    int rc = dafka_proto_recv (self->sub_msg, self->producer_sub);
    if (rc != 0)
        return 0;   // Unexpected message - ignore!

    switch (dafka_proto_id (self->sub_msg)) {
        case DAFKA_PROTO_ACK: {
            uint64_t ack_sequence = dafka_proto_sequence (self->sub_msg);
            if (self->verbose)
                zsys_debug ("Producer: Received ACK with sequence %u", ack_sequence);

            for (uint64_t index = self->last_acked_sequence + 1; index <= ack_sequence; index++) {
                zhashx_delete (self->message_cache, &index);
            }
            self->last_acked_sequence = ack_sequence;

            break;
        }
        case DAFKA_PROTO_FETCH: {
            const char *subject = dafka_proto_subject (self->sub_msg);
            const char *address = dafka_proto_topic (self->sub_msg);
            uint64_t sequence = dafka_proto_sequence (self->sub_msg);
            uint32_t count = dafka_proto_count (self->sub_msg);

            for (uint32_t index = 0; index < count; index++) {
                uint64_t lookup_key = sequence + index;
                dafka_proto_t *cached_msg = (dafka_proto_t *) zhashx_lookup (self->message_cache, &lookup_key);
                if (cached_msg) {
                    if (self->verbose)
                        zsys_info ("Producer: found answer for subscriber. Subject: %s, Partition: %s, Seq: %u",
                               subject, address, sequence + index);

                    dafka_proto_set_id (cached_msg, DAFKA_PROTO_DIRECT_RECORD);
                    dafka_proto_set_topic (cached_msg, dafka_proto_address (self->sub_msg));
                    dafka_proto_send (cached_msg, self->socket);
                }
                // No answer, exit the for loop
                else
                    break;
            }
            break;
        }
    }
    return 0;
}

//  Here we handle incoming message from the node

static int
s_recv_api (zloop_t *loop, zsock_t *pipe, void *arg)
{
    assert (loop);
    assert (pipe);
    assert (arg);
    dafka_producer_t *self = (dafka_producer_t  *) arg;

    char *command = zstr_recv (pipe);
    if (!command)
       return -1;       //  Interrupted

    int rc = 0;
    if (streq (command, "PUBLISH")) {
        zframe_t *content = NULL;
        zsock_brecv (pipe, "p", &content);
        s_publish (self, content);
    }
    else
    if (streq (command, "GET ADDRESS"))
        zsock_bsend (self->pipe, "p", dafka_proto_address (self->msg));
    else
    if (streq (command, "$TERM"))
        //  The $TERM command is send by zactor_destroy() method
        rc = -1;
    else {
        zsys_error ("invalid command '%s'", command);
        assert (false);
    }
    zstr_free (&command);
    return rc;
}


//  --------------------------------------------------------------------------
//  This is the actor which runs in its own thread.

void
dafka_producer (zsock_t *pipe, void *args)
{
    dafka_producer_args_t *pub_args = (dafka_producer_args_t *) args;
    dafka_producer_t * self = dafka_producer_new (pipe, pub_args);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (self->pipe, 0);

    if (self->verbose)
        zsys_info ("Producer: producer started");

    zloop_start (self->loop);

    bool verbose = self->verbose;
    dafka_producer_destroy (&self);

    if (verbose)
        zsys_info ("Producer: producer stopped");
}


//  --------------------------------------------------------------------------
//  Get the address the producer

const char *
dafka_producer_address (zactor_t *self) {
    zstr_send (self, "GET ADDRESS");
    const char *address;
    zsock_brecv (self, "p", &address);
    return address;
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
dafka_producer_test (bool verbose)
{
    printf (" * dafka_producer: ");
    //  @selftest
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
    //  @end

    printf ("OK\n");
}
