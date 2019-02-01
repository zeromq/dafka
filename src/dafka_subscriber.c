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
    dafka_proto_t *consumer_msg;// Reusable consumer message
    zsock_t *asker;             // Publisher to ask for missed messages
    zhashx_t *sequence_index;   // Index containing the latest sequence for each
                                // known publisher
    dafka_proto_t *ask_msg;   // Reusable publisher message
};

//  Static helper methods

static void
uint64_destroy (void **self_p) {
    assert (self_p);
    if (*self_p) {
        uint64_t *self = *self_p;
        free (self);
        *self_p = NULL;
    }
}

static void *
uint64_dup (const void *self) {
    uint64_t *value = malloc(sizeof (uint64_t));
    memcpy (value, self, sizeof (uint64_t));
    return value;
}

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
    char *addresses = ((char **) args)[0];
    char *consumer_pub_endpoint = ((char **) args)[1];
    self->socket = zsock_new_sub (addresses, NULL);
    zpoller_add (self->poller, self->socket);
    self->consumer_msg = dafka_proto_new ();

    self->sequence_index = zhashx_new ();
    zhashx_set_destructor(self->sequence_index, uint64_destroy);
    zhashx_set_duplicator (self->sequence_index, uint64_dup);

    self->asker = zsock_new_pub (consumer_pub_endpoint);
    self->ask_msg = dafka_proto_new ();
    dafka_proto_set_id (self->ask_msg, DAFKA_PROTO_ASK);
    zuuid_t *asker_uuid = zuuid_new ();
    dafka_proto_set_address (self->ask_msg, zuuid_str (asker_uuid));
    dafka_proto_subscribe (self->socket, DAFKA_PROTO_ANSWER, zuuid_str (asker_uuid));
    zuuid_destroy(&asker_uuid);

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
        dafka_proto_destroy (&self->consumer_msg);

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
    if (self->verbose)
        zsys_debug ("Subscribe to %s", topic);

    dafka_proto_subscribe (self->socket, DAFKA_PROTO_RELIABLE, topic);
}


//  Here we handle incoming message from the subscribtions

static void
dafka_subscriber_recv_subscriptions (dafka_subscriber_t *self)
{
    int rc = dafka_proto_recv (self->consumer_msg, self->socket);
    if (rc != 0)
       return;        //  Interrupted

    char id = dafka_proto_id (self->consumer_msg);
    const char *address = dafka_proto_address (self->consumer_msg);
    const char *topic = dafka_proto_topic (self->consumer_msg);
    zframe_t *content = dafka_proto_content (self->consumer_msg);
    uint64_t msg_sequence = dafka_proto_sequence (self->consumer_msg);

    // TODO: Extract into struct and/or add zstr_concat
    char *sequence_key = (char *) malloc(strlen (address) + strlen (topic) + 2);
    strcpy (sequence_key, topic);
    strcat (sequence_key, "/");
    strcat (sequence_key, address);

    // Check if we missed some messages
    uint64_t *last_known_sequence = (uint64_t *) zhashx_lookup (self->sequence_index, sequence_key);
    if (last_known_sequence && !(msg_sequence == *last_known_sequence + 1)) {
        uint64_t no_of_missed_messages = msg_sequence - *last_known_sequence;
        for (uint64_t index = 0; index < no_of_missed_messages; index++) {
            dafka_proto_set_subject (self->ask_msg, address);
            dafka_proto_set_sequence (self->ask_msg, (uint64_t) last_known_sequence + index + 1);
            dafka_proto_send (self->ask_msg, self->asker);
        }
    } else {
        if (id == DAFKA_PROTO_RELIABLE || id == DAFKA_PROTO_ANSWER) {
            zsock_bsend (self->pipe, "ssf", topic, address, content);
            zhashx_insert (self->sequence_index, sequence_key, &msg_sequence);
        }
    }
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

    char *consumer_args[] = {"inproc://hellopub", "inproc://helloasker"};
    zactor_t *sub = zactor_new (dafka_subscriber_actor, consumer_args);
    assert (sub);

    if (verbose)
        zstr_send (sub, "VERBOSE");

    zsock_send (sub, "ss", "SUBSCRIBE", "hello");
    usleep (100);

    zframe_t *content = zframe_new ("HELLO MATE", 10);
    int rc = dafka_publisher_publish (pub, &content);

    char *topic;
    char *address;
    zsock_brecv (sub, "ssf", &topic, &address, &content);
    char *content_str = zframe_strdup (content);
    assert (streq (topic, "hello"));
    assert (streq (content_str, "HELLO MATE"));
    zstr_free (&content_str);
    zframe_destroy (&content);

    dafka_publisher_destroy (&pub);
    zactor_destroy (&sub);
    //  @end

    printf ("OK\n");
}
