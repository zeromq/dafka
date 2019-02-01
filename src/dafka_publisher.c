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
    TODO:
        - Store send messages until an ACK has been received
@end
*/

#include "dafka_classes.h"

//  Structure of our actor

struct _dafka_publisher_t {
    //  Actor properties
    zsock_t *pipe;              //  Actor command pipe
    zloop_t *loop;                //  Event loop
    bool verbose;               //  Verbose logging enabled?
    //  Class properties
    zsock_t *socket;            // Socket to publish messages to
    dafka_proto_t *msg;         // Reusable MSG message to publish
    dafka_proto_t *head_msg;         // Reusable HEAD message to publish

    zactor_t *beacon;
};

static int
s_recv_api (zloop_t *loop, zsock_t *pipe, void *arg);

static int
s_recv_socket (zloop_t *loop, zsock_t *pipe, void *arg);

static int
s_recv_beacon (zloop_t *loop, zsock_t *pipe, void *arg);

static int
s_send_head (zloop_t *loop, int timer_id, void *arg);

//  --------------------------------------------------------------------------
//  Create a new dafka_publisher instance

static dafka_publisher_t *
dafka_publisher_new (zsock_t *pipe, dafka_publisher_args *args)
{
    dafka_publisher_t *self = (dafka_publisher_t *) zmalloc (sizeof (dafka_publisher_t));
    assert (self);
    assert (args);

    //  Initialize actor properties
    self->pipe = pipe;
    self->loop = zloop_new ();

    //  Initialize class properties
    self->socket = zsock_new_xpub (NULL);
    int port = zsock_bind (self->socket, "tcp://*:*");
    assert (self->socket);

    self->msg = dafka_proto_new ();
    dafka_proto_set_id (self->msg, DAFKA_PROTO_MSG);
    dafka_proto_set_topic (self->msg, args->topic);
    zuuid_t *address = zuuid_new ();
    dafka_proto_set_address (self->msg, zuuid_str (address));
    dafka_proto_set_sequence (self->msg, 0);

    self->head_msg = dafka_proto_new ();
    dafka_proto_set_id (self->head_msg, DAFKA_PROTO_HEAD);
    dafka_proto_set_topic (self->head_msg, args->topic);
    dafka_proto_set_address (self->head_msg, zuuid_str (address));

    self->beacon = zactor_new (dafka_beacon_actor, args->config);
    zsock_send (self->beacon, "ssi", "START", zuuid_str (address), port);
    assert (zsock_wait (self->beacon) == 0);

    zuuid_destroy (&address);

    zloop_reader (self->loop, self->socket, s_recv_socket, self);
    zloop_reader (self->loop, self->pipe, s_recv_api, self);
    zloop_reader (self->loop, zactor_sock (self->beacon), s_recv_beacon, self);
    size_t head_interval = atoi (zconfig_get (args->config, "head/interval", "1000"));
    zloop_timer (self->loop, head_interval, 0, s_send_head, self);
    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_publisher instance

static void
dafka_publisher_destroy (dafka_publisher_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_publisher_t *self = *self_p;

        //  Free class properties
        zsock_destroy (&self->socket);
        dafka_proto_destroy (&self->msg);
        dafka_proto_destroy (&self->head_msg);
        zactor_destroy (&self->beacon);

        //  Free actor properties
        zloop_destroy (&self->loop);
        free (self);
        *self_p = NULL;
    }
}


//  Publish content

static int
s_publish (dafka_publisher_t *self, zframe_t *content)
{
    assert (self);
    assert (content);

    if (self->verbose)
        zsys_debug ("Producer: Send MSG message with sequence %u", dafka_proto_sequence (self->msg));

    dafka_proto_set_content (self->msg, &content);
    int rc = dafka_proto_send (self->msg, self->socket);
    uint64_t sequence = dafka_proto_sequence (self->msg);
    dafka_proto_set_sequence (self->msg, sequence + 1);
    return rc;
}

//  Send a HEAD message

static int
s_send_head (zloop_t *loop, int timer_id, void *arg)
{
    assert (arg);
    dafka_publisher_t *self = (dafka_publisher_t  *) arg;
    dafka_proto_set_sequence (self->head_msg, dafka_proto_sequence (self->msg));
    if (self->verbose)
        zsys_debug ("Producer: Send HEAD message with sequence %u", dafka_proto_sequence (self->msg));

    return dafka_proto_send (self->head_msg, self->socket);
}

//  Here we handle incoming message from the beacon

static int
s_recv_beacon (zloop_t *loop, zsock_t *pipe, void *arg)
{
    assert (loop);
    assert (pipe);
    assert (arg);
    dafka_publisher_t *self = (dafka_publisher_t  *) arg;

    zmsg_t *msg = zmsg_recv (self->beacon);
    zmsg_destroy (&msg);

    // dafka_beacon_recv (self->beacon, self->producer_sub, self->verbose, "Producer");
}

static int
s_recv_socket (zloop_t *loop, zsock_t *pipe, void *arg) {
    assert (loop);
    assert (pipe);
    assert (arg);
    dafka_publisher_t *self = (dafka_publisher_t  *) arg;

    zmsg_t *msg = zmsg_recv (self->socket);
    zmsg_destroy (&msg);
}

//  Here we handle incoming message from the node

static int
s_recv_api (zloop_t *loop, zsock_t *pipe, void *arg)
{
    assert (loop);
    assert (pipe);
    assert (arg);
    dafka_publisher_t *self = (dafka_publisher_t  *) arg;

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
    if (streq (command, "VERBOSE"))
        self->verbose = true;
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
dafka_publisher_actor (zsock_t *pipe, void *args)
{
    dafka_publisher_args *pub_args = (dafka_publisher_args *) args;
    dafka_publisher_t * self = dafka_publisher_new (pipe, pub_args);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (self->pipe, 0);

    if (self->verbose)
        zsys_info ("Producer: Publisher started");

    zloop_start (self->loop);

    bool verbose = self->verbose;
    dafka_publisher_destroy (&self);

    if (verbose)
        zsys_info ("Producer: Publisher stopped");
}


//  --------------------------------------------------------------------------
//  Publish content

int
dafka_publisher_publish (zactor_t *self, zframe_t **content) {
    assert (*content);
    zstr_sendm (self, "PUBLISH");
    zsock_bsend (self, "p", *content);
    *content = NULL;
}

//  --------------------------------------------------------------------------
//  Get the address the publisher

const char *
dafka_publisher_address (zactor_t *self) {
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
dafka_publisher_test (bool verbose)
{
    printf (" * dafka_publisher: ");
    //  @selftest
    //  Simple create/destroy test
    zconfig_t *config = zconfig_new ("root", NULL);
    zconfig_put (config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/sub_address","inproc://tower-sub");
    zconfig_put (config, "beacon/pub_address","inproc://tower-pub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address","inproc://tower-sub");
    zconfig_put (config, "tower/pub_address","inproc://tower-pub");

    zactor_t *tower = zactor_new (dafka_tower_actor, config);

    dafka_publisher_args args;
    args.topic = strdup ("dummy");
    args.config = config;
    zactor_t *dafka_publisher = zactor_new (dafka_publisher_actor, &args);
    assert (dafka_publisher);

    zactor_destroy (&dafka_publisher);
    zactor_destroy (&tower);
    //  @end

    printf ("OK\n");
}
