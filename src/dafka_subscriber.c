/*  =========================================================================
    dafka_subscriber -

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
    dafka_subscriber -
@discuss
@end
*/

#include "dafka_classes.h"

//  Structure of our actor

struct _dafka_subscriber_t {
    //  Actor properties
    zsock_t *pipe;              //  Actor command pipe
    zpoller_t *poller;          //  Socket poller
    bool terminated;            //  Did caller ask us to quit?
    bool verbose;               //  Verbose logging enabled?
    //  Class properties
    zsock_t *socket;            // Socket to subscribe to messages
};


//  --------------------------------------------------------------------------
//  Create a new dafka_subscriber instance

static dafka_subscriber_t *
dafka_subscriber_new (zsock_t *pipe, void *args)
{
    dafka_subscriber_t *self = (dafka_subscriber_t *) zmalloc (sizeof (dafka_subscriber_t));
    assert (self);

    //  Initialize actor properties
    self->pipe = pipe;
    self->terminated = false;
    self->poller = zpoller_new (self->pipe, NULL);

    //  Initialize class properties
    char *addresses = (char *) args;
    self->socket = zsock_new_sub (addresses, NULL);
    zpoller_add (self->poller, self->socket);

    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_subscriber instance

static void
dafka_subscriber_destroy (dafka_subscriber_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_subscriber_t *self = *self_p;

        //  Free class properties
        zsock_destroy (&self->socket);

        //  Free actor properties
        self->terminated = true;
        zpoller_destroy (&self->poller);
        free (self);
        *self_p = NULL;
    }
}


//  Subscribe this actor to an topic. Return a value greater or equal to zero if
//  was successful. Otherwise -1.

static void
dafka_subscriber_subscribe (dafka_subscriber_t *self, const char *topic)
{
    assert (self);
    dafka_proto_subscribe (self->socket, DAFKA_PROTO_RELIABLE, topic);
}


//  Here we handle incoming message from the subscribtions

static void
dafka_subscriber_recv_subscriptions (dafka_subscriber_t *self)
{
    dafka_proto_t *msg;
    dafka_proto_recv (msg, self->socket);
    if (!msg)
       return;        //  Interrupted

    const char *address = dafka_proto_address (msg);
    const char *topic = dafka_proto_topic (msg);
    zframe_t *content = dafka_proto_content (msg);
    zsock_bsend (self->pipe, "ssf", topic, address, content);
    dafka_proto_destroy (&msg);
}

//  Here we handle incoming message from the node

static void
dafka_subscriber_recv_api (dafka_subscriber_t *self)
{
    //  Get the whole message of the pipe in one go
    zmsg_t *request = zmsg_recv (self->pipe);
    if (!request)
       return;        //  Interrupted

    char *command = zmsg_popstr (request);
    if (streq (command, "SUBSCRIBE")) {
        char *topic = zmsg_popstr (request);
        dafka_subscriber_subscribe (self, topic);
        zstr_free (&topic);
    }
    else
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


//  --------------------------------------------------------------------------
//  This is the actor which runs in its own thread.

void
dafka_subscriber_actor (zsock_t *pipe, void *args)
{
    dafka_subscriber_t * self = dafka_subscriber_new (pipe, args);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (self->pipe, 0);

    while (!self->terminated) {
        zsock_t *which = (zsock_t *) zpoller_wait (self->poller, 0);
        if (which == self->pipe)
            dafka_subscriber_recv_api (self);
        if (which == self->socket)
            dafka_subscriber_recv_subscriptions (self);
    }
    dafka_subscriber_destroy (&self);
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
dafka_subscriber_test (bool verbose)
{
    printf (" * dafka_subscriber: ");
    //  @selftest
    dafka_publisher_t *pub = dafka_publisher_new ("hello", "inproc://hellopub");
    assert (pub);

    zactor_t *sub = zactor_new (dafka_subscriber_actor, "inproc://hellopub");
    assert (sub);

    zsock_send (sub, "ss", "SUBSCRIBE", "hello");

    zframe_t *content = zframe_new ("HELLO", 5);
    int rc = dafka_publisher_publish (pub, content);

    char *topic;
    char *address;
    zsock_brecv (sub, "ssf", &topic, &address, &content);
    assert (streq (topic, "hello"));
    printf ("%s;%s", topic, address);
    zframe_destroy (&content);

    dafka_publisher_destroy (&pub);
    zactor_destroy (&sub);
    //  @end

    printf ("OK\n");
}
