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

size_t s_serialize_key (char *output, const char *subject, const char *address, uint64_t sequence) {
    size_t subject_size = strlen (subject);
    size_t address_size = strlen (address);

    char *needle = output;

    memcpy (needle, subject, subject_size);
    needle += subject_size;
    *needle = '\0';
    needle++;
    memcpy (needle, address, address_size);
    needle += address_size;
    *needle = '\0';
    needle++;

    // To actually have the keys ordered correctly we must encode uint64 as big endian
    size_t sequence_size = uint64_put_be ((byte *) needle, sequence);
    needle += sequence_size;

    return (needle - output);
}

void s_deserialize_key (const char *key, const char **subject, const char **address, uint64_t *sequence) {
    const char *needle = key;

    *subject = needle;
    needle += strlen (needle) + 1;
    *address = needle;
    needle += strlen (needle) + 1;
    uint64_get_be ((const byte *) needle, sequence);
}

void s_put_head (zhashx_t *heads, dafka_proto_t *msg) {
    char head_key[255 + 255 + 2];

    const char *subject = dafka_proto_subject (msg);
    const char *address = dafka_proto_topic (msg);

    snprintf (head_key, 255 + 255 + 1, "%s\\%s", subject, address);
    zhashx_update (heads, head_key, msg);
}

uint64_t s_get_head (zhashx_t *heads, leveldb_t *db, leveldb_readoptions_t *roptions,
                     const char *subject, const char *address) {

    char head_key[255 + 255 + 2];
    snprintf (head_key, 255 + 255 + 1, "%s\\%s", subject, address);

    dafka_proto_t *msg = (dafka_proto_t *) zhashx_lookup (heads, head_key);
    if (msg)
        return dafka_proto_sequence (msg);

    leveldb_iterator_t *iter = leveldb_create_iterator (db, roptions);

    // First, trying to find the last msg of subject and address
    char key[MAX_KEY_SIZE];
    size_t key_size = s_serialize_key (key, subject, address, NULL_SEQUENCE);
    leveldb_iter_seek (iter, key, key_size);

    // If valid, we need to take the iter one step back
    if (leveldb_iter_valid (iter))
        leveldb_iter_prev (iter);
        // If not valid, we check the last message in the db
    else
        leveldb_iter_seek_to_last (iter);

    // If db is empty, return NULL SEQUENCE
    if (!leveldb_iter_valid (iter)) {
        leveldb_iter_destroy (iter);
        return NULL_SEQUENCE;
    }

    // Check if the subject and topic match
    const char *head_subject;
    const char *head_address;
    uint64_t head_sequence;
    s_deserialize_key (leveldb_iter_key (iter, &key_size), &head_subject, &head_address, &head_sequence);

    // If the same subject and address, return the head
    if (streq (subject, head_subject) && streq (address, head_address)) {
        leveldb_iter_destroy (iter);
        return head_sequence;
    }

    leveldb_iter_destroy (iter);
    return NULL_SEQUENCE;
}

static void s_add_fetch (zhashx_t *fetches, dafka_proto_t *msg, const char *sender,
                         const char *subject, const char *address, uint64_t sequence, uint32_t count) {

    // Will only request 100,000 messages at a time
    if (count > 100000)
        count = 100000;

    dafka_proto_set_id (msg, DAFKA_PROTO_FETCH);
    dafka_proto_set_topic (msg, address);
    dafka_proto_set_subject (msg, subject);
    dafka_proto_set_sequence (msg, sequence);
    dafka_proto_set_count (msg, count);
    dafka_proto_set_address (msg, sender);

    char key[255 + 255 + 2];
    snprintf (key, 255 + 255 + 1, "%s\\%s", subject, address);

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

    char key[MAX_KEY_SIZE];

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
            size_t key_size = s_serialize_key (key, subject, address, sequence);
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

            char max_key[MAX_KEY_SIZE];
            size_t max_key_size = s_serialize_key (max_key, subject, address, sequence + count);

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

    char key[MAX_KEY_SIZE];
    zhashx_t *acks = zhashx_new ();
    zhashx_set_duplicator (acks, (zhashx_duplicator_fn *) dafka_proto_dup);
    zhashx_set_destructor (acks, (zhashx_destructor_fn *) dafka_proto_destroy);

    zhashx_t *fetches = zhashx_new ();
    zhashx_set_duplicator (acks, (zhashx_duplicator_fn *) dafka_proto_dup);
    zhashx_set_destructor (acks, (zhashx_destructor_fn *) dafka_proto_destroy);

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

                        uint64_t head = s_get_head (acks, self->db, roptions, subject, address);

                        if (head == NULL_SEQUENCE) {
                            uint32_t count = (uint32_t) (sequence + 1);

                            s_add_fetch (fetches, incoming_msg, self->address, subject, address, 0, count);
                        }
                        else
                        if (head < sequence) {
                            uint32_t count = (uint32_t) (sequence - head);

                            s_add_fetch (fetches, incoming_msg, self->address, subject, address, head + 1, count);
                        }
                        break;
                    }
                    case DAFKA_PROTO_DIRECT_MSG:
                    case DAFKA_PROTO_MSG: {
                        const char *subject = dafka_proto_subject (incoming_msg);
                        const char *address = dafka_proto_address (incoming_msg);
                        uint64_t sequence = dafka_proto_sequence (incoming_msg);
                        zframe_t *content = dafka_proto_content (incoming_msg);

                        uint64_t head = s_get_head (acks, self->db, roptions, subject, address);

                        if (head != NULL_SEQUENCE && sequence <= head) {
                            if (self->verbose)
                                zsys_debug ("Store: head at % "PRIu64 "dropping already received message %s %s %" PRIu64,
                                        head, subject, address, sequence);
                        }
                        else
                        if (head == NULL_SEQUENCE && sequence != 0) {
                            uint32_t count = (uint32_t) sequence + 1;
                            s_add_fetch (fetches, incoming_msg, self->address, subject, address, 0, count);
                        }
                        else
                        if (head != NULL_SEQUENCE && head + 1 != sequence) {
                            uint32_t count = (uint32_t) (sequence - head);
                            s_add_fetch (fetches, incoming_msg, self->address, subject, address, head + 1, count);
                        }
                        else {
                            // Saving to db
                            size_t key_size = s_serialize_key (key, subject, address, sequence);
                            leveldb_writebatch_put (batch, key, key_size, (const char *) zframe_data (content),
                                                    zframe_size (content));
                            batch_size++;

                            // Add the msg to the acks and send it after we save the batch
                            dafka_proto_set_id (incoming_msg, DAFKA_PROTO_ACK);
                            dafka_proto_set_topic (incoming_msg, address);
                            dafka_proto_set_subject (incoming_msg, subject);
                            dafka_proto_set_sequence (incoming_msg, sequence);
                            s_put_head (acks, incoming_msg);
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
            for (dafka_proto_t *ack_msg = (dafka_proto_t *) zhashx_first (acks);
                 ack_msg != NULL;
                 ack_msg = (dafka_proto_t *) zhashx_next (acks)) {
                dafka_proto_send (ack_msg, publisher);

                if (self->verbose)
                    zsys_info ("Store: Acked %s %s %" PRIu64,
                               dafka_proto_subject (ack_msg),
                               dafka_proto_topic (ack_msg),
                               dafka_proto_sequence (ack_msg));
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
            zhashx_purge (acks);
            zhashx_purge (fetches);

            if (self->verbose && batch_size)
                zsys_info ("Store: Saved batch of %d", batch_size);
        }
    }

    leveldb_writeoptions_destroy (woptions);
    leveldb_readoptions_destroy (roptions);
    zhashx_destroy (&acks);
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