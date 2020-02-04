/*  =========================================================================
    dafka_test_peer - Peer to test interact with producers, consumers and
                      stores.

    Copyright (c) the Contributors as noted in the AUTHORS file. This
    file is part of DAFKA, a decentralized distributed streaming
    platform: http://zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

/*
@header
    dafka_test_peer -
@discuss
@end
*/

#include "dafka_classes.h"

//  Structure of our actor

struct _dafka_test_peer_t {
    zsock_t *pipe;              //  Actor command pipe
    zpoller_t *poller;          //  Socket poller
    bool terminated;            //  Did caller ask us to quit?
    bool verbose;               //  Verbose logging enabled?

    //  Class properties
    zactor_t *beacon;           // Beacon actor
    zsock_t *sub;      // Subscriber
    zsock_t *pub;      // Publisher
    zuuid_t *address;
};

// Helper methods

static void
zmq_msg_freestr (void *data, void *hint) {
    char *str = (char *) data;
    zstr_free (&str);
}

//  --------------------------------------------------------------------------
//  Create a new dafka_test_peer instance

static dafka_test_peer_t *
dafka_test_peer_new (zsock_t *pipe, zconfig_t *config) {
    dafka_test_peer_t *self = (dafka_test_peer_t *) zmalloc (sizeof (dafka_test_peer_t));
    assert (self);

    //  Initialize actor properties
    self->pipe = pipe;
    self->terminated = false;

    //  Initialize class properties
    if (atoi (zconfig_get (config, "test/verbose", "0")))
        self->verbose = true;

    int hwm = atoi (zconfig_get (config, "consumer/high_watermark", "1000000"));

    self->sub = zsock_new_sub (NULL, NULL);
    zsock_set_rcvtimeo (self->sub, 0);
    zsock_set_rcvhwm (self->sub, hwm);

    self->pub = zsock_new_xpub (NULL);
    zsock_set_sndhwm (self->pub, hwm);
    zsock_set_xpub_verbose (self->pub, 1);
    int port = zsock_bind (self->pub, "tcp://*:*");
    assert (port != -1);

    self->address = zuuid_new ();
    zsock_set_subscribe (self->sub, ""); // Subscribe to all messages

    dafka_beacon_args_t beacon_args = {"Consumer", config};
    self->beacon = zactor_new (dafka_beacon_actor, &beacon_args);
    zsock_send (self->beacon, "ssi", "START", zuuid_str (self->address), port);

    self->poller = zpoller_new (self->pipe, self->sub, self->beacon, self->pub, NULL);

    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_test_peer instance

static void
dafka_test_peer_destroy (dafka_test_peer_t **self_p) {
    assert (self_p);
    if (*self_p) {
        dafka_test_peer_t *self = *self_p;

        //  Free class properties
        zpoller_destroy (&self->poller);
        zsock_destroy (&self->sub);
        zsock_destroy (&self->pub);
        zuuid_destroy (&self->address);
        zactor_destroy (&self->beacon);

        //  Free actor properties
        self->terminated = true;
        free (self);
        *self_p = NULL;
    }
}

//  Here we handle incoming message from the subscribtions

static void
dafka_test_peer_recv_sub (dafka_test_peer_t *self) {
    zmq_msg_t content;
    zmq_msg_init (&content);

    dafka_proto_t *msg = dafka_proto_new ();
    int rc = dafka_proto_recv (msg, self->sub);
    if (rc != 0) {
        zmq_msg_close (&content);
        return;        //  EAGAIN, Interrupted or malformed
    }

    dafka_proto_send (msg, self->pipe);
    dafka_proto_destroy (&msg);
}

//  Here we handle incoming message from the node

void
s_send_store_hello (dafka_test_peer_t *self) {
    char *consumer_address = zstr_recv (self->pipe);
    dafka_proto_t *shello_msg = dafka_proto_new ();
    dafka_proto_set_id (shello_msg, DAFKA_PROTO_STORE_HELLO);
    dafka_proto_set_address (shello_msg, zuuid_str (self->address));
    dafka_proto_set_topic (shello_msg, consumer_address);

    dafka_proto_send (shello_msg, self->pub);
    if (self->verbose)
        zsys_debug ("Test Peer: Send STORE-HELLO");

    zstr_free (&consumer_address);
    dafka_proto_destroy (&shello_msg);
}

void
s_send_head (dafka_test_peer_t *self) {
    char *topic;
    uint64_t sequence;
    zsock_recv (self->pipe, "s8", &topic, &sequence);

    dafka_proto_t *head_msg = dafka_proto_new ();
    dafka_proto_set_id (head_msg, DAFKA_PROTO_HEAD);
    dafka_proto_set_topic (head_msg, topic);
    dafka_proto_set_subject (head_msg, topic);
    dafka_proto_set_address (head_msg, zuuid_str (self->address));
    dafka_proto_set_sequence (head_msg, sequence);

    zstr_free (&topic);

    dafka_proto_send (head_msg, self->pub);
    dafka_proto_destroy (&head_msg);
    if (self->verbose)
        zsys_debug ("Test Peer: Send HEAD");
}

void
s_send_msg (dafka_test_peer_t *self) {
    char *topic, *content;
    uint64_t sequence;
    zsock_recv (self->pipe, "s8s", &topic, &sequence, &content);

    zmq_msg_t content_msg;
    zmq_msg_init_data (&content_msg, content, strlen (content), zmq_msg_freestr, NULL);

    dafka_proto_t *record_msg = dafka_proto_new ();
    dafka_proto_set_id (record_msg, DAFKA_PROTO_RECORD);
    dafka_proto_set_topic (record_msg, topic);
    dafka_proto_set_subject (record_msg, topic);
    dafka_proto_set_address (record_msg, zuuid_str (self->address));
    dafka_proto_set_sequence (record_msg, sequence);
    dafka_proto_set_content (record_msg, &content_msg);

    zmq_msg_close (&content_msg);
    zstr_free (&topic);
    /* zstr_free (&content); */

    dafka_proto_send (record_msg, self->pub);
    dafka_proto_destroy (&record_msg);
    if (self->verbose)
        zsys_debug ("Test Peer: Send RECORD");
}

static void
dafka_test_peer_recv_api (dafka_test_peer_t *self) {
    //  Read only the command frame of the message
    char *command = zstr_recv (self->pipe);
    if (!command)
        return;        //  Interrupted

    if (streq (command, "HEAD"))
        s_send_head (self);
    else if (streq (command, "RECORD"))
        s_send_msg (self);
    else if (streq (command, "STORE-HELLO"))
        s_send_store_hello (self);
    else if (streq (command, "$TERM"))
        //  The $TERM command is send by zactor_destroy() method
        self->terminated = true;
    else {
        zsys_error ("invalid command '%s'", command);
        assert (false);
    }
    zstr_free (&command);
}

// Here we handle subscriptions from xpub

static void
dafka_test_peer_recv_pub (dafka_test_peer_t *self) {
    dafka_proto_t *msg = dafka_proto_new ();
    int rc = dafka_proto_recv (msg, self->pub);
    dafka_proto_destroy (&msg);
    if (rc == -1)
        return;

}


//  --------------------------------------------------------------------------
//  This is the actor which runs in its own thread.

void
dafka_test_peer (zsock_t *pipe, void *args) {
    dafka_test_peer_t *self = dafka_test_peer_new (pipe, (zconfig_t *) args);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (self->pipe, 0);

    if (self->verbose)
        zsys_info ("Test Peer: running...");

    while (!self->terminated) {
        void *which = (zsock_t *) zpoller_wait (self->poller, -1);
        if (which == self->sub)
            dafka_test_peer_recv_sub (self);
        if (which == self->pipe)
            dafka_test_peer_recv_api (self);
        if (which == self->beacon)
            dafka_beacon_recv (self->beacon, self->sub, self->verbose, "Test Peer");
        if (which == self->pub)
            dafka_test_peer_recv_pub (self);
    }
    bool verbose = self->verbose;
    dafka_test_peer_destroy (&self);

    if (verbose)
        zsys_info ("Test Peer: stopped");
}

void
dafka_test_peer_send_head (zactor_t *self, char *topic, uint64_t sequence) {
    assert (self);
    zsock_send (self, "ss8", "HEAD", topic, sequence);
}

void
dafka_test_peer_send_store_hello (zactor_t *self, char *consumer_address) {
    assert (self);
    zstr_sendm (self, "STORE-HELLO");
    zstr_send (self, consumer_address);
}

void
dafka_test_peer_send_record (zactor_t *self, char *topic, uint64_t sequence, char *content) {
    assert (self);
    zsock_send (self, "ss8s", "RECORD", topic, sequence, content);
}

dafka_proto_t *
dafka_test_peer_recv (zactor_t *self) {
    dafka_proto_t *msg = dafka_proto_new ();
    dafka_proto_recv (msg, zactor_sock (self));
    return msg;
}

void
assert_consumer_hello_msg (dafka_proto_t *msg, int no_of_subjects) {
    assert (dafka_proto_id (msg) == DAFKA_PROTO_CONSUMER_HELLO);
    assert (zlist_size (dafka_proto_subjects (msg)) == no_of_subjects);
    dafka_proto_destroy (&msg);
}

void
assert_get_heads_msg (dafka_proto_t *msg, char *topic) {
    assert (dafka_proto_id (msg) == DAFKA_PROTO_GET_HEADS);
    assert (streq (dafka_proto_topic (msg), topic));
    dafka_proto_destroy (&msg);
}

void
assert_fetch_msg (dafka_proto_t *msg, char *topic, uint64_t sequence) {
    assert (dafka_proto_id (msg) == DAFKA_PROTO_FETCH);
    assert (streq (dafka_proto_subject (msg), topic));
    assert (dafka_proto_sequence (msg) == sequence);
    dafka_proto_destroy (&msg);
}

void
assert_consumer_msg (dafka_consumer_msg_t *msg, char *topic, char *content) {
    assert (streq (dafka_consumer_msg_subject (msg), topic));
    assert (dafka_consumer_msg_streq (msg, content));
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
dafka_test_peer_test (bool verbose) {
    printf (" * dafka_test_peer: ");
    //  @selftest
    zconfig_t *config = zconfig_new ("root", NULL);
    zconfig_put (config, "test/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/interval", "50");
    zconfig_put (config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/sub_address", "inproc://consumer-tower-sub");
    zconfig_put (config, "beacon/pub_address", "inproc://consumer-tower-pub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address", "inproc://consumer-tower-sub");
    zconfig_put (config, "tower/pub_address", "inproc://consumer-tower-pub");

    zconfig_put (config, "consumer/offset/reset", "earliest");
    zactor_t *tower = zactor_new (dafka_tower_actor, config);

    zactor_t *testpeer = zactor_new (dafka_test_peer, config);
    assert (testpeer);

    zconfig_destroy (&config);
    zactor_destroy (&tower);
    zactor_destroy (&testpeer);

    printf ("OK\n");
}
