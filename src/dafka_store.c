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
#include <leveldb/c.h>

#define MAX_KEY_SIZE  (255 + 255 + 8 + 5)

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

    leveldb_t *db;
    leveldb_options_t *dboptions;
    leveldb_writeoptions_t *woptions;
    leveldb_readoptions_t *roptions;

    char *address;
};


//  --------------------------------------------------------------------------
//  Create a new dafka_store instance

static dafka_store_t *
dafka_store_new (zsock_t *pipe, zconfig_t *config)
{
    dafka_store_t *self = (dafka_store_t *) zmalloc (sizeof (dafka_store_t));
    assert (self);

    if (atoi (zconfig_get (config, "store/verbose", "0")))
        self->verbose = true;

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
    dafka_proto_subscribe (self->sub, DAFKA_PROTO_MSG, self->address);
    dafka_proto_subscribe (self->sub, DAFKA_PROTO_FETCH, "");
    dafka_proto_subscribe (self->sub, DAFKA_PROTO_HEAD, "");

    self->beacon = zactor_new (dafka_beacon_actor, config);
    zsock_send (self->beacon, "ssi", "START", self->address, port);
    assert (zsock_wait (self->beacon) == 0);

    // Configure and open the leveldb database for the store
    const char *db_path = zconfig_get (config, "store/db", "storedb");
    char *err = NULL;
    self->dboptions = leveldb_options_create ();
    leveldb_options_set_create_if_missing (self->dboptions, 1);
    self->db = leveldb_open (self->dboptions, db_path, &err);
    if (err) {
        zsys_error ("Store: failed to open db %s", err);
        assert (false);
    }
    self->woptions = leveldb_writeoptions_create();
    self->roptions = leveldb_readoptions_create ();

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

        zpoller_destroy (&self->poller);
        zstr_free (&self->address);
        zsock_destroy (&self->sub);
        zsock_destroy (&self->pub);
        dafka_proto_destroy (&self->income_msg);
        dafka_proto_destroy (&self->outgoing_msg);
        leveldb_readoptions_destroy (self->roptions);
        leveldb_writeoptions_destroy (self->woptions);
        leveldb_close (self->db);
        leveldb_options_destroy (self->dboptions);
        self->roptions = NULL;
        self->woptions = NULL;
        self->db = NULL;
        self->dboptions = NULL;
        zactor_destroy (&self->beacon);

        //  Free object itself
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

size_t s_serialize_content_key (char* output, const char* subject, const char* address, uint64_t sequence) {
    return (size_t) snprintf (output, MAX_KEY_SIZE, "C\\%s\\%s\\%" PRIu64, subject, address, sequence);
}

size_t s_serialize_head_key (char* output, const char* subject, const char* address) {
    return (size_t) snprintf (output, MAX_KEY_SIZE, "H\\%s\\%s", subject, address);
}

void s_free_value (void **hint) {
    char *value = (char *) *hint;
    leveldb_free (value);

    *hint = NULL;
}

static uint64_t
dafka_store_get_head (dafka_store_t *self, char* key, size_t key_size) {
    size_t head_size;
    char *err = NULL;
    uint64_t *head_sequence = (uint64_t*) leveldb_get (self->db, self->roptions, key, key_size, &head_size, &err);
    if (err) {
        zsys_error ("Store: failed to get head %s", err);
        assert (false);
    }

    if (head_sequence == NULL)
        return (uint64_t)-1;

    uint64_t value = *head_sequence;
    leveldb_free (head_sequence);

    return value;
}

static void dafka_store_send_fetch (dafka_store_t *self,
        uint64_t head_sequence, const char* subject, const char* address, uint64_t sequence) {
    uint32_t count;

    if (head_sequence == - 1)
        count = (uint32_t)(sequence + 1);
    else
        count = (uint32_t)(sequence - head_sequence);

    if (self->verbose)
        zsys_info ("Store: store is missing %d messages. Subject: %s, Partition: %s, Seq: %u", count, subject, address, sequence);

    // Will only request 1000 messages at a time
    if (count > 1000)
        count = 1000;

    dafka_proto_set_id (self->outgoing_msg,  DAFKA_PROTO_FETCH);
    dafka_proto_set_topic (self->outgoing_msg, address);
    dafka_proto_set_subject (self->outgoing_msg, subject);
    dafka_proto_set_sequence (self->outgoing_msg, head_sequence + 1);
    dafka_proto_set_count (self->outgoing_msg, count);
    dafka_proto_set_address (self->outgoing_msg, self->address);
    dafka_proto_send (self->outgoing_msg, self->pub);
}

// Handle messages from network

static void
dafka_store_recv_sub (dafka_store_t *self) {
    int rc = dafka_proto_recv (self->income_msg, self->sub);
    if (rc == -1) //  Interrupted
        return;

    char key[MAX_KEY_SIZE];

    switch (dafka_proto_id (self->income_msg)) {
        case DAFKA_PROTO_HEAD: {
            const char *subject = dafka_proto_subject (self->income_msg);
            const char *address = dafka_proto_address (self->income_msg);
            uint64_t sequence = dafka_proto_sequence (self->income_msg);

            size_t head_key_size = s_serialize_head_key (key, subject, address);
            uint64_t head_sequence = dafka_store_get_head (self, key, head_key_size);

            if (head_sequence == -1 || head_sequence < sequence)
                dafka_store_send_fetch (self, head_sequence, subject, address, sequence);

            break;
        }
        case DAFKA_PROTO_MSG: {
            const char *subject = dafka_proto_subject (self->income_msg);
            const char *address = dafka_proto_address (self->income_msg);
            uint64_t sequence = dafka_proto_sequence (self->income_msg);

            size_t head_key_size = s_serialize_head_key (key, subject, address);
            uint64_t head_sequence = dafka_store_get_head (self, key, head_key_size);

            if (head_sequence != -1 && sequence <= head_sequence) {
                // We already have the message
                if (self->verbose)
                    zsys_debug ("Store: message already received. Subject: %s, Partition: %s, Seq: %u",
                            subject, address, sequence);
                return;
            }

            if (head_sequence + 1 != sequence) {
                dafka_store_send_fetch (self, head_sequence, subject, address, sequence);
                return;
            }

            // Saving head to database
            char *err = NULL;
            leveldb_put (self->db, self->woptions, key, head_key_size, (char*) &sequence, sizeof (uint64_t), &err);
            if (err) {
                zsys_error ("Store: failed to update head to db %s", err);
                assert (false);
            }

            // Saving msg to database
            size_t key_size = s_serialize_content_key (key, subject, address, sequence);
            zframe_t *value = dafka_proto_content (self->income_msg);
            leveldb_put (self->db, self->woptions, key,  key_size,
                         (char *) zframe_data (value), zframe_size (value), &err);
            if (err) {
                zsys_error ("Store: failed to save message to db %s", err);
                assert (false);
            }

            if (self->verbose)
                zsys_info ("Store: storing a message. Subject: %s, Partition: %s, Seq: %u", subject, address, sequence);

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
            dafka_proto_set_id (self->outgoing_msg, DAFKA_PROTO_MSG);

            for (uint32_t i = 0; i < count; i++) {
                size_t key_size = s_serialize_content_key (key, subject, address, sequence + i);

                // Read data from db
                char *err = NULL;
                size_t value_size;
                char* value = leveldb_get (self->db, self->roptions, key, key_size, &value_size, &err);
                if (err) {
                    zsys_error ("Store: failed to load msg from db %s", err);
                    assert (false);
                }

                if (value) {
                    if (self->verbose)
                        zsys_info ("Store: found answer for subscriber. Subject: %s, Partition: %s, Seq: %u, size %u",
                               subject, address, sequence + i, value_size);

                    // Creating the frame without duplicate the data, frame will free the value when destroyed
                    zframe_t *frame = zframe_frommem (value, value_size, s_free_value, value);

                    // The answer topic is the asker address
                    dafka_proto_set_sequence (self->outgoing_msg, sequence + i);
                    dafka_proto_set_content (self->outgoing_msg, &frame);
                    dafka_proto_send (self->outgoing_msg, self->pub);
                } else {
                    if (self->verbose)
                        zsys_info ("Store: no answer for subscriber. Subject: %s, Partition: %s, Seq: %u",
                               subject, address, sequence + i);
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
    dafka_store_t * self = dafka_store_new (pipe, (zconfig_t *) arg);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (self->pipe, 0);

    if (self->verbose)
        zsys_info ("Store: running...");

    while (!self->terminated) {
        void *which = (zsock_t *) zpoller_wait (self->poller, -1);
        if (which == self->pipe)
            dafka_store_recv_api (self);
        if (which == self->sub)
            dafka_store_recv_sub (self);
        if (which == self->beacon)
            dafka_beacon_recv (self->beacon, self->sub, self->verbose, "Store");
    }

    if (self->verbose)
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
    zconfig_put (config, "beacon/sub_address","inproc://store-tower-sub");
    zconfig_put (config, "beacon/pub_address","inproc://store-tower-pub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address","inproc://store-tower-sub");
    zconfig_put (config, "tower/pub_address","inproc://store-tower-pub");
    zconfig_put (config, "store/verbose", verbose ? "1" : "0");
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
    zclock_sleep (100);

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
    dafka_producer_msg_destroy (&p_msg);
    zactor_destroy (&consumer);
    zactor_destroy (&store);
    zactor_destroy (&tower);
    zactor_destroy (&producer);
    zconfig_destroy (&config);
    //  @end

    printf ("OK\n");
}
