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

#define NULL_SEQUENCE  0xFFFFFFFFFFFFFFFF

//  Structure of our actor

struct _dafka_store_t {
    bool verbose;               //  Verbose logging enabled?

    leveldb_t *db;
    leveldb_options_t *dboptions;

    zconfig_t *config;
    char *address;
};


//  --------------------------------------------------------------------------
//  Create a new dafka_store instance

static dafka_store_t *
dafka_store_new (zconfig_t *config) {
    dafka_store_t *self = (dafka_store_t *) zmalloc (sizeof (dafka_store_t));
    assert (self);

    self->config = config;

    if (atoi (zconfig_get (config, "store/verbose", "0")))
        self->verbose = true;

    self->address = generate_address ();

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

    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_store instance

static void
dafka_store_destroy (dafka_store_t **self_p) {
    assert (self_p);
    if (*self_p) {
        dafka_store_t *self = *self_p;

        zstr_free (&self->address);
        leveldb_close (self->db);
        leveldb_options_destroy (self->dboptions);
        self->db = NULL;
        self->dboptions = NULL;

        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}

uint64_t s_get_head (leveldb_t *db, leveldb_readoptions_t *roptions,
                     zhashx_t *heads, dafka_head_key_t *key) {
    uint64_t *sequence_p = (uint64_t *) zhashx_lookup (heads, key);
    if (sequence_p)
        return *sequence_p;

    size_t key_size;
    const char *key_bytes = dafka_head_key_encode (key, &key_size);

    size_t value_size;
    char* err = NULL;
    char *value = leveldb_get (db, roptions, key_bytes, key_size, &value_size, &err);
    if (err) {
        zsys_error ("Error to get head from db %s", err);
        assert (false);
    }

    if (value == NULL)
        return NULL_SEQUENCE;

    assert (value_size == sizeof (uint64_t));

    uint64_t sequence;
    uint64_get_be ((const byte *)value, &sequence);

    return sequence;
}

static void s_add_fetch (zhashx_t *fetches,
        dafka_head_key_t * key,
        const char *sender,
        uint64_t sequence, uint32_t count) {

    // Will only request 100,000 messages at a time
    if (count > 100000)
        count = 100000;

    dafka_proto_t *msg = dafka_proto_new ();

    dafka_proto_set_id (msg, DAFKA_PROTO_FETCH);
    dafka_proto_set_topic (msg, dafka_head_key_address (key));
    dafka_proto_set_subject (msg, dafka_head_key_subject (key));
    dafka_proto_set_sequence (msg, sequence);
    dafka_proto_set_count (msg, count);
    dafka_proto_set_address (msg, sender);

    zhashx_update (fetches, key, msg);
}

void
dafka_store_read_actor (zsock_t *pipe, dafka_store_t *self) {
    zsock_t *publisher = zsock_new_pub (NULL);
    int port = zsock_bind (publisher, "tcp://*:*");
    char *reader_address = generate_address ();

    dafka_beacon_args_t beacon_args = {"Store Reader", self->config};
    zactor_t *beacon = zactor_new (dafka_beacon_actor, &beacon_args);
    zsock_send (beacon, "ssi", "START", reader_address, port);

    zsock_t *subscriber = zsock_new_sub (NULL, NULL);
    dafka_proto_subscribe (subscriber, DAFKA_PROTO_FETCH, "");

    dafka_proto_t *incoming_msg = dafka_proto_new ();
    dafka_proto_t *outgoing_msg = dafka_proto_new ();

    zpoller_t *poller = zpoller_new (subscriber, beacon, pipe, publisher, NULL);

    zsock_signal (pipe, 0);

    dafka_msg_key_t *msg_key = dafka_msg_key_new ();

    bool terminated = false;
    while (!terminated) {
        void *which = zpoller_wait (poller, -1);

        if (which == beacon)
            dafka_beacon_recv (beacon, subscriber, self->verbose, "Store Reader");
        if (which == pipe) {
            char *command = zstr_recv (pipe);

            if (streq(command, "$TERM"))
                terminated = true;

            zstr_free (&command);
        }
        if (which == subscriber) {
            int rc = dafka_proto_recv (incoming_msg, subscriber);
            if (rc == -1)
                continue;

            const char *sender = dafka_proto_address (incoming_msg);
            const char *subject = dafka_proto_subject (incoming_msg);;
            const char *address = dafka_proto_topic (incoming_msg);
            uint64_t sequence = dafka_proto_sequence (incoming_msg);
            uint32_t count = dafka_proto_count (incoming_msg);

            // Ignore messages from the write actor
            if (streq (sender, self->address))
                continue;

            const leveldb_snapshot_t *snapshot = leveldb_create_snapshot (self->db);
            leveldb_readoptions_t *roptions = leveldb_readoptions_create ();
            leveldb_readoptions_set_snapshot (roptions, snapshot);
            leveldb_iterator_t *iter = leveldb_create_iterator (self->db, roptions);

            dafka_msg_key_set (msg_key, subject, address, sequence);

            size_t key_size;
            const char *key = dafka_msg_key_encode (msg_key, &key_size);
            leveldb_iter_seek (iter, key, key_size);

            if (!leveldb_iter_valid (iter)) {
                if (self->verbose)
                    zsys_info ("Store: no answer for consumer. Subject: %s, Address: %s, Seq: %" PRIu64,
                               subject, address, sequence);

                leveldb_iter_destroy (iter);
                leveldb_readoptions_destroy (roptions);
                leveldb_release_snapshot (self->db, snapshot);

                continue;
            }

            // For now we only sending from what the user requested, so if the sequence at the
            // beginning doesn't match, don't send anything
            // TODO: mark the first message as tail
            size_t iter_key_size;
            const char *iter_key = leveldb_iter_key (iter, &iter_key_size);
            if (iter_key_size != key_size || memcmp (iter_key, key, key_size) != 0) {
                if (self->verbose)
                    zsys_info ("Store: no answer for consumer. Subject: %s, Address: %s, Seq: %" PRIu64,
                               subject, address, sequence);

                leveldb_iter_destroy (iter);
                leveldb_readoptions_destroy (roptions);
                leveldb_release_snapshot (self->db, snapshot);

                continue;
            }

            size_t max_key_size;
            dafka_msg_key_set (msg_key, subject, address, sequence + count);
            const char *max_key = dafka_msg_key_encode (msg_key, &max_key_size);

            dafka_proto_set_topic (outgoing_msg, sender);
            dafka_proto_set_subject (outgoing_msg, subject);
            dafka_proto_set_address (outgoing_msg, address);
            dafka_proto_set_id (outgoing_msg, DAFKA_PROTO_DIRECT_MSG);

            uint64_t iter_sequence = sequence;

            while (max_key_size == iter_key_size && memcmp (iter_key, max_key, max_key_size) < 0) {
                size_t content_size;
                const char *content = leveldb_iter_value (iter, &content_size);
                zframe_t *frame = zframe_new (content, content_size);

                dafka_proto_set_sequence (outgoing_msg, iter_sequence);
                dafka_proto_set_content (outgoing_msg, &frame);
                dafka_proto_send (outgoing_msg, publisher);

                if (self->verbose)
                    zsys_info ("Store: found answer for consumer. Subject: %s, Partition: %s, Seq: %lu",
                            subject, address, iter_sequence);

                iter_sequence++;
                leveldb_iter_next (iter);

                if (!leveldb_iter_valid (iter))
                    break;

                iter_key = leveldb_iter_key (iter, &iter_key_size);
            }

            leveldb_iter_destroy (iter);
            leveldb_readoptions_destroy (roptions);
            leveldb_release_snapshot (self->db, snapshot);
        }
    }

    dafka_msg_key_destroy (&msg_key);
    dafka_proto_destroy (&incoming_msg);
    dafka_proto_destroy (&outgoing_msg);
    zsock_destroy (&subscriber);
    zpoller_destroy (&poller);
    zactor_destroy (&beacon);
    zstr_free (&reader_address);
    zsock_destroy (&publisher);
}

void
dafka_store_write_actor (zsock_t *pipe, dafka_store_t *self) {
    zsock_t *publisher = zsock_new_pub (NULL);
    int port = zsock_bind (publisher, "tcp://*:*");

    dafka_beacon_args_t beacon_args = {"Store Writer", self->config};
    zactor_t *beacon = zactor_new (dafka_beacon_actor, &beacon_args);
    zsock_send (beacon, "ssi", "START", self->address, port);

    zsock_t *direct_sub = zsock_new_sub (NULL, NULL);
    dafka_proto_subscribe (direct_sub, DAFKA_PROTO_DIRECT_MSG, self->address);

    zsock_t *pubsub_sub = zsock_new_sub (NULL, NULL);
    dafka_proto_subscribe (pubsub_sub, DAFKA_PROTO_MSG, "");
    dafka_proto_subscribe (pubsub_sub, DAFKA_PROTO_HEAD, "");

    dafka_proto_t *incoming_msg = dafka_proto_new ();
    dafka_proto_t *outgoint_msg = dafka_proto_new ();

    zpoller_t *poller = zpoller_new (direct_sub, pubsub_sub, publisher, beacon, pipe, NULL);

    dafka_msg_key_t *msg_key = dafka_msg_key_new ();
    dafka_head_key_t *head_key = dafka_head_key_new ();

    zhashx_t *heads = zhashx_new ();
    dafka_head_key_hashx_set (heads);
    zhashx_set_duplicator (heads, uint64_dup);
    zhashx_set_destructor (heads, uint64_destroy);

    zhashx_t *fetches = zhashx_new ();
    dafka_head_key_hashx_set (fetches);
    zhashx_set_duplicator (fetches, NULL);
    zhashx_set_destructor (fetches, (zhashx_destructor_fn *) dafka_proto_destroy);

    leveldb_readoptions_t *roptions = leveldb_readoptions_create ();
    leveldb_writeoptions_t *woptions = leveldb_writeoptions_create ();

    zsock_signal (pipe, 0);

    bool terminated = false;
    while (!terminated) {
        void *which = zpoller_wait (poller, -1);

        if (which == beacon) {
            char *command = zstr_recv (beacon);
            if (command == NULL)
                continue;

            char *address = zstr_recv (beacon);

            if (streq (command, "CONNECT")) {
                if (self->verbose)
                    zsys_info ("Store: Connecting to %s", address);

                zsock_connect (direct_sub, "%s", address);
                zsock_connect (pubsub_sub, "%s", address);
            }
            else
            if (streq (command, "DISCONNECT")) {
                zsock_disconnect (direct_sub, "%s", address);
                zsock_disconnect (pubsub_sub, "%s", address);
            }
            else {
                zsys_error ("Transport: Unknown command %s", command);
                assert (false);
            }

            zstr_free (&address);
            zstr_free (&command);
        }
        if (which == pipe) {
            char *command = zstr_recv (pipe);

            if (command == NULL)
                continue;

            if (streq(command, "$TERM"))
                terminated = true;

            zstr_free (&command);
        }

        if (which == direct_sub || which == pubsub_sub) {
            int batch_size = 0;
            leveldb_writebatch_t *batch = leveldb_writebatch_create ();

            while (batch_size < 100000 && (zsock_has_in (direct_sub) || zsock_has_in (pubsub_sub))) {
                int rc;

                if (zsock_has_in (direct_sub))
                    rc = dafka_proto_recv (incoming_msg, direct_sub);
                else
                    rc = dafka_proto_recv (incoming_msg, pubsub_sub);

                if (rc == -1)
                    break;

                switch (dafka_proto_id (incoming_msg)) {
                    case DAFKA_PROTO_HEAD: {
                        const char *subject = dafka_proto_subject (incoming_msg);
                        const char *address = dafka_proto_address (incoming_msg);
                        uint64_t sequence = dafka_proto_sequence (incoming_msg);

                        dafka_head_key_set (head_key, subject, address);

                        uint64_t head = s_get_head (self->db, roptions, heads, head_key);

                        if (head == NULL_SEQUENCE) {
                            uint32_t count = (uint32_t) (sequence + 1);

                            s_add_fetch (fetches, head_key, self->address, 0, count);
                        }
                        else
                        if (head < sequence) {
                            uint32_t count = (uint32_t) (sequence - head);

                            s_add_fetch (fetches, head_key, self->address, head + 1, count);
                        }
                        break;
                    }
                    case DAFKA_PROTO_DIRECT_MSG:
                    case DAFKA_PROTO_MSG: {
                        const char *subject = dafka_proto_subject (incoming_msg);
                        const char *address = dafka_proto_address (incoming_msg);
                        uint64_t sequence = dafka_proto_sequence (incoming_msg);
                        zframe_t *content = dafka_proto_content (incoming_msg);

                        dafka_head_key_set (head_key, subject, address);
                        uint64_t head = s_get_head (self->db, roptions, heads, head_key);

                        if (head != NULL_SEQUENCE && sequence <= head) {
                            if (self->verbose)
                                zsys_debug ("Store: head at % "PRIu64 "dropping already received message %s %s %" PRIu64,
                                        head, subject, address, sequence);
                        }
                        else
                        if (head == NULL_SEQUENCE && sequence != 0) {
                            uint32_t count = (uint32_t) sequence + 1;
                            s_add_fetch (fetches, head_key, self->address, 0, count);
                        }
                        else
                        if (head != NULL_SEQUENCE && head + 1 != sequence) {
                            uint32_t count = (uint32_t) (sequence - head);
                            s_add_fetch (fetches, head_key, self->address, head + 1, count);
                        }
                        else {
                            // Saving msg to db
                            dafka_msg_key_set (msg_key, subject, address, sequence);
                            size_t msg_key_size;
                            const char *msg_key_bytes = dafka_msg_key_encode (msg_key, &msg_key_size);
                            leveldb_writebatch_put (batch,
                                    msg_key_bytes, msg_key_size,
                                    (const char *) zframe_data (content), zframe_size (content));

                            // update the head
                            char sequence_bytes[sizeof(uint64_t)];
                            size_t sequence_size = uint64_put_be ((byte *) sequence_bytes, sequence);
                            size_t head_key_size;
                            const char *head_key_bytes = dafka_head_key_encode (head_key, &head_key_size);
                            leveldb_writebatch_put (batch,
                                    head_key_bytes, head_key_size,
                                    sequence_bytes, sequence_size); // TODO; we might be override it multiple times in a batch
                            batch_size++;

                            // Update the head in the heads hash
                            zhashx_update (heads, head_key, &sequence);
                        }

                        break;
                    }
                    default:
                        break;
                }
            }

            char *err = NULL;
            leveldb_write (self->db, woptions, batch, &err);
            if (err) {
                zsys_error ("Store: failed to save batch to db %s", err);
                assert (false);
            }
            leveldb_writebatch_destroy (batch);

            // Sending the acks now
            dafka_proto_set_address (outgoint_msg, self->address);
            for (uint64_t *head_p = (uint64_t *) zhashx_first (heads);
                 head_p != NULL;
                 head_p = (uint64_t *) zhashx_next (heads)) {

                uint64_t head = *head_p;
                dafka_head_key_t *current_head_key =
                        (dafka_head_key_t *) zhashx_cursor (heads);

                dafka_proto_set_id (outgoint_msg, DAFKA_PROTO_ACK);
                dafka_proto_set_topic (outgoint_msg, dafka_head_key_address (current_head_key));
                dafka_proto_set_subject (outgoint_msg, dafka_head_key_subject (current_head_key));
                dafka_proto_set_sequence (outgoint_msg, head);
                dafka_proto_send (outgoint_msg, publisher);

                if (self->verbose)
                    zsys_info ("Store: Acked %s %s %" PRIu64,
                               dafka_head_key_subject (current_head_key),
                               dafka_head_key_address (current_head_key),
                               head);
            }

            // Sending the fetches now
            for (dafka_proto_t *fetch_msg = (dafka_proto_t *) zhashx_first (fetches);
                 fetch_msg != NULL;
                 fetch_msg = (dafka_proto_t *) zhashx_next (fetches)) {
                dafka_proto_send (fetch_msg, publisher);

                if (self->verbose)
                    zsys_info ("Store: Fetching %d from %s %s %" PRIu64,
                               dafka_proto_count (fetch_msg),
                               dafka_proto_subject (fetch_msg),
                               dafka_proto_topic (fetch_msg),
                               dafka_proto_sequence (fetch_msg));
            }

            // Batch is done, clearing all the acks and fetches
            zhashx_purge (heads);
            zhashx_purge (fetches);

            if (self->verbose && batch_size)
                zsys_info ("Store: Saved batch of %d", batch_size);
        }
    }

    leveldb_writeoptions_destroy (woptions);
    leveldb_readoptions_destroy (roptions);
    zhashx_destroy (&heads);
    zhashx_destroy (&fetches);
    dafka_proto_destroy (&incoming_msg);
    dafka_proto_destroy (&outgoint_msg);
    zsock_destroy (&pubsub_sub);
    zsock_destroy (&direct_sub);
    zpoller_destroy (&poller);
    zactor_destroy (&beacon);
    zsock_destroy (&publisher);
}

//  --------------------------------------------------------------------------
//  This is the actor which runs in its own thread.

void
dafka_store_actor (zsock_t *pipe, void *arg) {
    dafka_store_t *self = dafka_store_new ((zconfig_t *) arg);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (pipe, 0);

    if (self->verbose)
        zsys_info ("Store: running...");

    zactor_t *writer = zactor_new ((zactor_fn *) dafka_store_write_actor, self);
    zactor_t *reader = zactor_new ((zactor_fn *) dafka_store_read_actor, self);

    bool terminated = false;
    while (!terminated) {
        char *command = zstr_recv (pipe);

        if (command == NULL)
            continue;

        if (streq (command, "$TERM"))
            terminated = true;

        zstr_free (&command);
    }

    zactor_destroy (&reader);
    zactor_destroy (&writer);

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
dafka_store_test (bool verbose) {
    printf (" * dafka_store: ");
    //  @selftest
    //  Simple create/destroy test
    zconfig_t *config = zconfig_new ("root", NULL);
    zconfig_put (config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/sub_address", "inproc://store-tower-sub");
    zconfig_put (config, "beacon/pub_address", "inproc://store-tower-pub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address", "inproc://store-tower-sub");
    zconfig_put (config, "tower/pub_address", "inproc://store-tower-pub");
    zconfig_put (config, "store/verbose", verbose ? "1" : "0");
    zconfig_put (config, "consumer/verbose", verbose ? "1" : "0");
    zconfig_put (config, "producer/verbose", verbose ? "1" : "0");
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
    zclock_sleep (2000);

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
    zactor_destroy (&consumer);
    dafka_producer_msg_destroy (&p_msg);
    zactor_destroy (&producer);
    zactor_destroy (&store);
    zactor_destroy (&tower);
    zconfig_destroy (&config);
    //  @end

    printf ("OK\n");
}