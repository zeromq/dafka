/*  =========================================================================
    dafka_consumer -

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
    dafka_consumer -
@discuss
    TODO:
      - Send earliest message when a store connects
      - We must not send FETCH on every message, the problem is, that if you
        missed something, and there is high rate, you will end up sending a
        lot of fetch messages for same address
      - Prioritize DIRECT_MSG messages over MSG this will avoid discrding MSGs
        when catching up
@end
*/

#include "dafka_classes.h"

//  Structure of our actor

struct _dafka_consumer_t {
    //  Actor properties
    zsock_t *pipe;                      //  Actor command pipe
    zpoller_t *poller;                  //  Socket poller
    bool terminated;                    //  Did caller ask us to quit?
    bool verbose;                       //  Verbose logging enabled?
    //  Class properties
    zsock_t *consumer_sub;              // Subscriber to get messages from topics
    dafka_proto_t *consumer_msg;        // Reusable consumer message

    zsock_t *consumer_pub;              // Publisher to ask for missed messages
    dafka_proto_t *pub_msg;             // Reusable message for receiving xpub subscriptions
    zhashx_t *sequence_index;           // Index containing the latest sequence for each known publisher
    dafka_proto_t *get_heads_msg;       // Reusable get heads message
    dafka_proto_t *hello_msg;           // Reusable hello message
    zactor_t *beacon;                   // Beacon actor
    bool reset_latest;                  // Wheather to process records from earliest or latest
    zlist_t *subjects;                  // List of topics the consumer is subscribed for
    dafka_fetch_filter_t *fetch_filter; // Filter to not repeat fetch requests
};

//  --------------------------------------------------------------------------
//  Create a new dafka_consumer instance

static dafka_consumer_t *
dafka_consumer_new (zsock_t *pipe, zconfig_t *config) {
    dafka_consumer_t *self = (dafka_consumer_t *) zmalloc (sizeof (dafka_consumer_t));
    assert (self);

    //  Initialize actor properties
    self->pipe = pipe;
    self->terminated = false;
    self->reset_latest = streq (zconfig_get (config, "consumer/offset/reset", "latest"), "latest");

    //  Initialize class properties
    if (atoi (zconfig_get (config, "consumer/verbose", "0")))
        self->verbose = true;

    int hwm = atoi (zconfig_get (config, "consumer/high_watermark", "1000000"));

    self->consumer_sub = zsock_new_sub (NULL, NULL);
    zsock_set_rcvtimeo (self->consumer_sub, 0);
    zsock_set_rcvhwm (self->consumer_sub, hwm);
    self->consumer_msg = dafka_proto_new ();

    self->sequence_index = zhashx_new ();
    zhashx_set_destructor (self->sequence_index, uint64_destroy);
    zhashx_set_duplicator (self->sequence_index, uint64_dup);

    self->consumer_pub = zsock_new_xpub (NULL);
    zsock_set_sndhwm (self->consumer_pub, hwm);
    zsock_set_xpub_verbose (self->consumer_pub, 1);
    int port = zsock_bind (self->consumer_pub, "tcp://*:*");
    assert (port != -1);
    self->pub_msg = dafka_proto_new ();

    zuuid_t *consumer_address = zuuid_new ();
    self->get_heads_msg = dafka_proto_new ();
    dafka_proto_set_id (self->get_heads_msg, DAFKA_PROTO_GET_HEADS);
    dafka_proto_set_address (self->get_heads_msg, zuuid_str (consumer_address));

    self->hello_msg = dafka_proto_new ();
    dafka_proto_set_id (self->hello_msg, DAFKA_PROTO_CONSUMER_HELLO);
    dafka_proto_set_address (self->hello_msg, zuuid_str (consumer_address));

    dafka_proto_subscribe (self->consumer_sub, DAFKA_PROTO_DIRECT_RECORD, zuuid_str (consumer_address));
    dafka_proto_subscribe (self->consumer_sub, DAFKA_PROTO_DIRECT_HEAD, zuuid_str (consumer_address));
    dafka_proto_subscribe (self->consumer_sub, DAFKA_PROTO_STORE_HELLO, zuuid_str (consumer_address));

    dafka_beacon_args_t beacon_args = {"Consumer", config};
    self->beacon = zactor_new (dafka_beacon_actor, &beacon_args);
    zsock_send (self->beacon, "ssi", "START", zuuid_str (consumer_address), port);

    self->subjects = zlist_new ();
    zlist_autofree (self->subjects);

    self->fetch_filter = dafka_fetch_filter_new (self->consumer_pub, zuuid_str (consumer_address), self->verbose);
    zuuid_destroy (&consumer_address);

    self->poller = zpoller_new (self->pipe, self->consumer_sub, self->beacon, self->consumer_pub, NULL);

    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_consumer instance

static void
dafka_consumer_destroy (dafka_consumer_t **self_p) {
    assert (self_p);
    if (*self_p) {
        dafka_consumer_t *self = *self_p;

        //  Free class properties
        zpoller_destroy (&self->poller);
        dafka_fetch_filter_destroy (&self->fetch_filter);
        zlist_destroy (&self->subjects);
        zsock_destroy (&self->consumer_sub);
        zsock_destroy (&self->consumer_pub);

        dafka_proto_destroy (&self->consumer_msg);
        dafka_proto_destroy (&self->get_heads_msg);
        dafka_proto_destroy (&self->hello_msg);
        dafka_proto_destroy (&self->pub_msg);
        zhashx_destroy (&self->sequence_index);
        zactor_destroy (&self->beacon);

        //  Free actor properties
        self->terminated = true;
        free (self);
        *self_p = NULL;
    }
}


static void
s_send_get_heads_msg (dafka_consumer_t *self, const char *topic) {
    assert (self);
    assert (topic);
    if (self->verbose)
        zsys_debug ("Consumer: Send EARLIEST message for topic %s", topic);

    dafka_proto_set_topic (self->get_heads_msg, topic);
    dafka_proto_send (self->get_heads_msg, self->consumer_pub);
}


static void
s_send_consumer_hello_msg (dafka_consumer_t *self, const char *store_address) {
    assert (self);
    assert (store_address);

    zlist_t *subjects;

    if (!self->reset_latest)
        subjects = zlist_dup (self->subjects);
    else
        subjects = zlist_new ();

    dafka_proto_set_subjects (self->hello_msg, &subjects);
    dafka_proto_set_topic (self->hello_msg, store_address);
    dafka_proto_send (self->hello_msg, self->consumer_pub);
}


//  Subscribe this actor to an topic. Return a value greater or equal to zero if
//  was successful. Otherwise -1.

static void
s_subscribe (dafka_consumer_t *self, const char *topic) {
    assert (self);
    if (self->verbose)
        zsys_debug ("Consumer: Subscribe to topic %s", topic);

    dafka_proto_subscribe (self->consumer_sub, DAFKA_PROTO_RECORD, topic);
    dafka_proto_subscribe (self->consumer_sub, DAFKA_PROTO_HEAD, topic);

    if (!self->reset_latest)
        s_send_get_heads_msg (self, topic);

    zlist_append (self->subjects, (void *) topic);
}


//  Here we handle incoming message from the subscribtions

static void
dafka_consumer_recv_sub (dafka_consumer_t *self) {
    char sequence_key[256 + 1 + 256 + 1];

    for (int i = 0; i < 100000; ++i) {
        int rc = dafka_proto_recv(self->consumer_msg, self->consumer_sub);
        if (rc != 0)
            return;        //  EAGAIN, Interrupted or malformed

        char id = dafka_proto_id(self->consumer_msg);
        zframe_t *content = dafka_proto_content(self->consumer_msg);
        uint64_t current_sequence = dafka_proto_sequence(self->consumer_msg);
        const char *address = dafka_proto_address(self->consumer_msg);
        const char *subject = dafka_proto_subject(self->consumer_msg);

        if (self->verbose)
            zsys_debug("Consumer: Received message %c from %s on subject %s with sequence %u",
                       id, address, subject, current_sequence);

        snprintf (sequence_key, sizeof (sequence_key), "%s/%s", subject, address);

        // TODO: Get partition tail through EARLIEST message
        uint64_t *last_known_sequence_p = (uint64_t *) zhashx_lookup(self->sequence_index, sequence_key);
        uint64_t last_known_sequence = -1;
        bool last_sequence_known = last_known_sequence_p != NULL;
        if (last_known_sequence_p)
            last_known_sequence = *last_known_sequence_p;

        switch (dafka_proto_id(self->consumer_msg)) {
            case DAFKA_PROTO_RECORD:
            case DAFKA_PROTO_DIRECT_RECORD: {
                if (!last_sequence_known) {
                    if (self->reset_latest) {
                        if (self->verbose)
                            zsys_debug("Consumer: Setting offset for topic %s on partition %s to latest %u",
                                       subject,
                                       address,
                                       current_sequence - 1);

                        // Set to latest - 1 in order to process the current message
                        last_known_sequence = current_sequence - 1;
                        zhashx_insert(self->sequence_index, sequence_key, &last_known_sequence);
                    } else {
                        if (self->verbose)
                            zsys_debug("Consumer: Setting offset for topic %s on partition %s to earliest %u",
                                       subject,
                                       address,
                                       last_known_sequence);

                        zhashx_insert(self->sequence_index, sequence_key, &last_known_sequence);
                    }
                }

                //  Check if we missed some messages
                if (current_sequence > last_known_sequence + 1)
                    dafka_fetch_filter_send(self->fetch_filter, subject, address, last_known_sequence + 1);

                if (current_sequence == last_known_sequence + 1) {
                    if (self->verbose)
                        zsys_debug("Consumer: Send message %u to client", current_sequence);

                    zhashx_update(self->sequence_index, sequence_key, &current_sequence);
                    zsock_bsend(self->pipe, "ssf", subject, address, content);
                }
                break;
            }
            case DAFKA_PROTO_HEAD:
            case DAFKA_PROTO_DIRECT_HEAD: {
                if (!last_sequence_known) {
                    if (self->reset_latest) {
                        if (self->verbose)
                            zsys_debug("Consumer: Setting offset for topic %s on partition %s to latest %u",
                                       subject,
                                       address,
                                       current_sequence);

                        // Set to latest in order to skip fetching older messages
                        last_known_sequence = current_sequence;
                        zhashx_insert(self->sequence_index, sequence_key, &last_known_sequence);
                        last_sequence_known = true;
                    }
                }

                //  Check if we missed some messages
                if (!last_sequence_known || current_sequence > last_known_sequence)
                    dafka_fetch_filter_send(self->fetch_filter, subject, address, last_known_sequence + 1);

                break;
            }
            case DAFKA_PROTO_STORE_HELLO: {
                const char *store_address = dafka_proto_address(self->consumer_msg);

                if (self->verbose)
                    zsys_info("Consumer: Consumer is connected to store %s", store_address);

                s_send_consumer_hello_msg(self, store_address);

                break;
            }
            default:
                return;     // Unexpected message id

        }
    }
}

//  Here we handle incoming message from the node

static void
dafka_consumer_recv_api (dafka_consumer_t *self) {
    //  Get the whole message of the pipe in one go
    zmsg_t *request = zmsg_recv (self->pipe);
    if (!request)
        return;        //  Interrupted

    char *command = zmsg_popstr (request);
    if (streq (command, "SUBSCRIBE")) {
        char *topic = zmsg_popstr (request);
        s_subscribe (self, topic);
        zstr_free (&topic);
    } else if (streq (command, "$TERM"))
        //  The $TERM command is send by zactor_destroy() method
        self->terminated = true;
    else {
        zsys_error ("invalid command '%s'", command);
        assert (false);
    }
    zstr_free (&command);
    zmsg_destroy (&request);
}

// Here we handle subscriptions from xpub

static void
dafka_consumer_recv_pub (dafka_consumer_t *self) {
    int rc = dafka_proto_recv (self->pub_msg, self->consumer_pub);
    if (rc == -1)
        return;

    // If a store just subscribed for DIRECT GET HEADS we will issue a new get heads message
    if (dafka_proto_id (self->pub_msg) == DAFKA_PROTO_CONSUMER_HELLO &&
        dafka_proto_is_subscribe (self->pub_msg)) {

        const char *store_address = dafka_proto_topic (self->pub_msg);

        if (self->verbose)
            zsys_info ("Consumer: Store %s is connected to consumer", store_address);

        s_send_consumer_hello_msg (self, store_address);
    }
}


//  --------------------------------------------------------------------------
//  This is the actor which runs in its own thread.

void
dafka_consumer (zsock_t *pipe, void *args) {
    dafka_consumer_t *self = dafka_consumer_new (pipe, (zconfig_t *) args);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (self->pipe, 0);

    if (self->verbose)
        zsys_info ("Consumer: running...");

    while (!self->terminated) {
        void *which = (zsock_t *) zpoller_wait (self->poller, -1);
        if (which == self->consumer_sub)
            dafka_consumer_recv_sub (self);
        if (which == self->pipe)
            dafka_consumer_recv_api (self);
        if (which == self->beacon)
            dafka_beacon_recv (self->beacon, self->consumer_sub, self->verbose, "Consumer");
        if (which == self->consumer_pub)
            dafka_consumer_recv_pub (self);
    }
    bool verbose = self->verbose;
    dafka_consumer_destroy (&self);

    if (verbose)
        zsys_info ("Consumer: stopped");
}

//  --------------------------------------------------------------------------
//  Subscribe to a topic

int
dafka_consumer_subscribe (zactor_t *actor, const char *subject) {
    return zsock_send (actor, "ss", "SUBSCRIBE", subject);
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
dafka_consumer_test (bool verbose) {
    printf (" * dafka_consumer: ");
    //  @selftest
    // ----------------------------------------------------
    //  Cleanup old test artifacts
    // ----------------------------------------------------
    if (zsys_file_exists (SELFTEST_DIR_RW "/storedb")) {
        zdir_t *store_dir = zdir_new (SELFTEST_DIR_RW "/storedb", NULL);
        zdir_remove (store_dir, true);
        zdir_destroy (&store_dir);
    }

    // ----------------------------------------------------
    // Test with consumer.offset.reset = earliest
    // ----------------------------------------------------
    zconfig_t *config = zconfig_new ("root", NULL);
    zconfig_put (config, "beacon/interval", "50");
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

    dafka_producer_args_t pub_args = {"hello", config};
    zactor_t *producer = zactor_new (dafka_producer, &pub_args);
    assert (producer);

    zactor_t *store = zactor_new (dafka_store_actor, config);
    assert (store);

    zactor_t *consumer = zactor_new (dafka_consumer, config);
    assert (consumer);
    zclock_sleep (250);

    dafka_producer_msg_t *p_msg = dafka_producer_msg_new ();
    dafka_producer_msg_set_content_str (p_msg, "HELLO MATE");
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
    zclock_sleep (
            100);  // Make sure the first two messages have been received from the store and the consumer is now up to date

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
    assert (streq (dafka_consumer_msg_subject (c_msg), "hello"));
    assert (dafka_consumer_msg_streq (c_msg, "HELLO ATEM"));

    dafka_producer_msg_destroy (&p_msg);
    dafka_consumer_msg_destroy (&c_msg);
    zactor_destroy (&tower);
    zactor_destroy (&producer);
    zactor_destroy (&consumer);
    zconfig_destroy (&config);

    // ----------------------------------------------------
    //  Cleanup test artifacts
    // ----------------------------------------------------
    zdir_t *store_dir = zdir_new (SELFTEST_DIR_RW "/storedb", NULL);
    zdir_remove (store_dir, true);
    zdir_destroy (&store_dir);
    //  @end

    printf ("OK\n");
}
