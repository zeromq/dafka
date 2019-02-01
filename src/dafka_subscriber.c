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
    zsock_t *socket;            // Subscriber to get messages from topics
    dafka_proto_t *consumer_msg;// Reusable consumer message

    zsock_t *consumer_pub;      // Publisher to ask for missed messages
    zhashx_t *sequence_index;   // Index containing the latest sequence for each
                                // known publisher
    dafka_proto_t *fetch_msg;   // Reusable publisher message
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

    self->consumer_pub = zsock_new_pub (consumer_pub_endpoint);
    self->fetch_msg = dafka_proto_new ();
    dafka_proto_set_id (self->fetch_msg, DAFKA_PROTO_FETCH);
    zuuid_t *consumer_address = zuuid_new ();
    dafka_proto_set_address (self->fetch_msg, zuuid_str (consumer_address));
    dafka_proto_subscribe (self->socket, DAFKA_PROTO_DIRECT, zuuid_str (consumer_address));
    zuuid_destroy(&consumer_address);

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

    dafka_proto_subscribe (self->socket, DAFKA_PROTO_MSG, topic);
}


//  Here we handle incoming message from the subscribtions

static void
dafka_subscriber_recv_subscriptions (dafka_subscriber_t *self)
{
    int rc = dafka_proto_recv (self->consumer_msg, self->socket);
    if (rc != 0)
       return;        //  Interrupted

    char id = dafka_proto_id (self->consumer_msg);
    zframe_t *content = dafka_proto_content (self->consumer_msg);
    uint64_t msg_sequence = dafka_proto_sequence (self->consumer_msg);

    const char *address;
    const char *subject;
    if (id == DAFKA_PROTO_MSG) {
        address = dafka_proto_address (self->consumer_msg);
        subject = dafka_proto_topic (self->consumer_msg);
    }
    else
    if (id == DAFKA_PROTO_DIRECT) {
        address = dafka_proto_address (self->consumer_msg);
        subject = dafka_proto_subject (self->consumer_msg);
    }
    else
        return;     // Unexpected message id

    // TODO: Extract into struct and/or add zstr_concat
    char *sequence_key = (char *) malloc (strlen (address) + strlen (subject) + 2);
    strcpy (sequence_key, subject);
    strcat (sequence_key, "/");
    strcat (sequence_key, address);

    if (self->verbose)
        zsys_debug ("Received message %c from %s on subject %s with sequence %u",
                    id, address, subject, msg_sequence);

    // Check if we missed some messages
    uint64_t *last_known_sequence = (uint64_t *) zhashx_lookup (self->sequence_index, sequence_key);
    if (!last_known_sequence) {
        last_known_sequence = malloc (sizeof (uint64_t));
        *last_known_sequence = -1;
    }

    if (id == DAFKA_PROTO_MSG && !(msg_sequence == *last_known_sequence + 1)) {
        uint64_t no_of_missed_messages = msg_sequence - *last_known_sequence;
        if (self->verbose)
            zsys_debug ("FETCHING %u messages on subject %s from %s starting at sequence %u",
                        no_of_missed_messages,
                        subject,
                        address,
                        *last_known_sequence + 1);

        dafka_proto_set_subject (self->fetch_msg, subject);
        dafka_proto_set_topic (self->fetch_msg, address);
        dafka_proto_set_sequence (self->fetch_msg, (uint64_t) *last_known_sequence + 1);
        dafka_proto_set_count (self->fetch_msg, no_of_missed_messages);
        dafka_proto_send (self->fetch_msg, self->consumer_pub);
    }

    if (msg_sequence == *last_known_sequence + 1) {
        if (self->verbose)
            zsys_debug ("Send message %u to client", msg_sequence);

        zhashx_insert (self->sequence_index, sequence_key, &msg_sequence);
        zsock_bsend (self->pipe, "ssf", subject, address, content);
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

    if (self->verbose)
        zsys_info ("Subscriber: running...");

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

    char *store_args[] = {"inproc://hellostore", "inproc://hellopub,inproc://hellofetcher"};
    zactor_t *store = zactor_new (dafka_store_actor, store_args);
    assert (store);

    char *consumer_args[] = {"inproc://hellopub,inproc://hellostore", "inproc://hellofetcher"};
    zactor_t *sub = zactor_new (dafka_subscriber_actor, consumer_args);
    assert (sub);

    if (verbose) {
        zstr_send (store, "VERBOSE");
        zstr_send (sub, "VERBOSE");
    }

    zframe_t *content = zframe_new ("HELLO MATE", 10);
    int rc = dafka_publisher_publish (pub, &content);
    assert (rc == 0);
    sleep (1);  // Make sure message is published before subscriber subscribes

    rc = zsock_send (sub, "ss", "SUBSCRIBE", "hello");
    assert (rc == 0);
    sleep (1);  // Make sure subscription is active before sending the next message

    // This message is discarded but triggers a FETCH from the store
    content = zframe_new ("HELLO ATEM", 10);
    rc = dafka_publisher_publish (pub, &content);
    assert (rc == 0);
    sleep (1);  // Make sure the first two messages have been received from the store and the subscriber is now up to date

    content = zframe_new ("HELLO ATEM", 10);
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

    dafka_publisher_destroy (&pub);
    zactor_destroy (&sub);
    //  @end

    printf ("OK\n");
}
