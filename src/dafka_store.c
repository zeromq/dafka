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
dafka_store_new (zconfig_t *config)
{
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
dafka_store_destroy (dafka_store_t **self_p)
{
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

size_t s_serialize_key (char* output, const char* subject, const char* address, uint64_t sequence) {
    size_t subject_size = strlen (subject);
    size_t address_size = strlen (address);

    char* needle = output;

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

void s_deserialize_key (const char *key, const char** subject, const char **address, uint64_t *sequence) {
    const char *needle = key;

    *subject = needle;
    needle += strlen (needle) + 1;
    *address = needle;
    needle += strlen (needle) + 1;
    uint64_get_be ((const byte*) needle, sequence);
}

void s_put_head (zhashx_t *heads, dafka_proto_t *msg) {
    char head_key[255+255+2];

    const char *subject = dafka_proto_subject (msg);
    const char *address = dafka_proto_topic (msg);

    snprintf (head_key, 255 + 255 + 1, "%s\\%s", subject, address);
    zhashx_update (heads, head_key, msg);
}

uint64_t s_get_head (zhashx_t *heads, leveldb_t *db, leveldb_readoptions_t *roptions,
        const char *subject, const char* address) {

    char head_key[255+255+2];
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
    if (!leveldb_iter_valid (iter))  {
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

static void s_send_fetch (dafka_proto_t *msg, zsock_t *publisher, const char *sender,
        const char* subject, const char* address, uint64_t sequence, uint32_t count) {

    // Will only request 1000 messages at a time
    if (count > 1000)
        count = 1000;

    dafka_proto_set_id (msg,  DAFKA_PROTO_FETCH);
    dafka_proto_set_topic (msg, address);
    dafka_proto_set_subject (msg, subject);
    dafka_proto_set_sequence (msg, sequence);
    dafka_proto_set_count (msg, count);
    dafka_proto_set_address (msg, sender);
    dafka_proto_send (msg, publisher);
}

void
dafka_store_read_actor (zsock_t *pipe, dafka_store_t *self) {
    zsock_t *publisher = zsock_new_pub (NULL);
    int port = zsock_bind (publisher, "tcp://*:*");
    char *reader_address = generate_address ();

    zactor_t *beacon = zactor_new (dafka_beacon_actor, self->config);
    zsock_send (beacon, "ssi", "START", reader_address, port);
    assert (zsock_wait (beacon) == 0);

    dafka_proto_t *msg = dafka_proto_new ();
    zpoller_t *poller = zpoller_new (publisher, beacon, pipe, NULL);

    zsock_signal (pipe, 0);

    bool terminated = false;

    char key[MAX_KEY_SIZE];

    char *command = NULL;

    while (!terminated) {
        void *which = zpoller_wait (poller, -1);

        if (which == beacon) {
            // We actually don't have a subscriber, we are only using the beacon for publishing,
            // we just need to drop the subscription
            zmsg_t *drop = zmsg_recv (beacon);
            zmsg_destroy (&drop);
        }
        if (which == pipe) {
            zstr_free (&command);
            command = zstr_recv (pipe);

            if (streq(command, "$TERM"))
                terminated = true;
            else if (streq (command, "FETCH")) {
                const char *sender;
                const char *subject;
                const char *address;
                uint64_t sequence;
                uint32_t count;
                int rc = zsock_brecv (pipe, "sss84", &sender, &subject, &address, &sequence, &count);
                if (rc == -1) {
                    zstr_free (&command);
                    continue;
                }

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

                dafka_proto_set_topic (msg, sender);
                dafka_proto_set_subject (msg, subject);
                dafka_proto_set_address (msg, address);
                dafka_proto_set_id (msg, DAFKA_PROTO_MSG);

                uint64_t iter_sequence = sequence;

                while (max_key_size == iter_key_size && memcmp (iter_key, max_key, max_key_size) < 0) {
                    size_t content_size;
                    const char* content = leveldb_iter_value (iter, &content_size);
                    zframe_t *frame = zframe_new (content, content_size);

                    dafka_proto_set_sequence (msg, iter_sequence);
                    dafka_proto_set_content (msg, &frame);
                    dafka_proto_send (msg, publisher);

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
    }

    zstr_free (&command);


    dafka_proto_destroy (&msg);
    zpoller_destroy (&poller);
    zactor_destroy (&beacon);
    zstr_free (&reader_address);
    zsock_destroy (&publisher);
}

void
dafka_store_write_actor (zsock_t *pipe, dafka_store_t *self) {
    zsock_t *publisher = zsock_new_pub (NULL);
    int port = zsock_bind (publisher, "tcp://*:*");

    zactor_t *beacon = zactor_new (dafka_beacon_actor, self->config);
    zsock_send (beacon, "ssi", "START", self->address, port);
    assert (zsock_wait (beacon) == 0);

    dafka_proto_t *msg = dafka_proto_new ();

    zpoller_t *poller = zpoller_new (publisher, beacon, pipe, NULL);

    char key[MAX_KEY_SIZE];
    zhashx_t *acks = zhashx_new ();
    zhashx_set_duplicator (acks, (zhashx_duplicator_fn *) dafka_proto_dup);
    zhashx_set_destructor (acks, (zhashx_destructor_fn *) dafka_proto_destroy);

    leveldb_readoptions_t *roptions = leveldb_readoptions_create ();
    leveldb_writeoptions_t *woptions = leveldb_writeoptions_create ();

    zsock_signal (pipe, 0);

    bool terminated = false;
    while (!terminated) {
        void *which = zpoller_wait (poller, -1);

        if (which == beacon) {
            // We actually don't have a subscriber, we are only using the beacon for publishing,
            // we just need to drop the subscription
            zmsg_t *drop = zmsg_recv (beacon);
            zmsg_destroy (&drop);
        }
        if (which == pipe) {
            leveldb_writebatch_t *batch = leveldb_writebatch_create ();
            int batch_size = 0;

            char *command = NULL;
            zframe_t *content = NULL;

            while (zsock_has_in (pipe)) {
                zstr_free (&command);
                command = zstr_recv (pipe);

                if (streq(command, "$TERM"))
                    terminated = true;
                else if (streq (command, "HEAD")) {
                    const char *subject;
                    const char *address;
                    uint64_t sequence;
                    int rc = zsock_brecv (pipe, "ss8", &subject, &address, &sequence);
                    if (rc == -1)
                        continue;

                    uint64_t head = s_get_head (acks, self->db, roptions, subject, address);

                    if (head == NULL_SEQUENCE) {
                        uint32_t count = (uint32_t) (sequence + 1);

                        s_send_fetch (msg, publisher, self->address, subject, address, 0, count);

                        if (self->verbose)
                            zsys_info ("Store: missing %d messages from %s %s 0",
                                       count, subject, address);
                    } else if (head < sequence) {
                        uint32_t count = (uint32_t) (sequence - head);

                        s_send_fetch (msg, publisher, self->address, subject, address, head + 1, (uint32_t) sequence);

                        if (self->verbose)
                            zsys_info ("Store: missing %d messages from sequence %s %s %" PRIu64,
                                       count, subject, address, head + 1);
                    }
                } else if (streq (command, "MSG")) {
                    const char *subject;
                    const char *address;
                    uint64_t sequence;
                    zframe_destroy (&content);
                    int rc = zsock_brecv (pipe, "ss8p", &subject, &address, &sequence, &content);
                    if (rc == -1)
                        continue;

                    uint64_t head = s_get_head (acks, self->db, roptions, subject, address);

                    if (head != NULL_SEQUENCE && sequence <= head) {
                        if (self->verbose)
                            zsys_debug ("Store: dropping already received message %s %s %" PRIu64, subject, address,
                                        sequence);

                        continue;
                    }

                    if (head == NULL_SEQUENCE && sequence != 0) {
                        uint32_t count = (uint32_t) sequence + 1;
                        s_send_fetch (msg, publisher, self->address, subject, address, 0, count);

                        if (self->verbose)
                            zsys_info ("Store: missing %d messages from sequence %s %s 0", count, subject, address);

                        continue;
                    }

                    if (head != NULL_SEQUENCE && head + 1 != sequence) {
                        uint32_t count = (uint32_t) (sequence - head);
                        s_send_fetch (msg, publisher, self->address, subject, address, head + 1, count);

                        if (self->verbose)
                            zsys_info ("Store: missing %d messages from sequence %s %s %" PRIu64,
                                       count, subject, address, head + 1);

                        continue;
                    }

                    // Saving to db
                    size_t key_size = s_serialize_key (key, subject, address, sequence);
                    leveldb_writebatch_put (batch, key, key_size, (const char *) zframe_data (content), zframe_size (content));
                    batch_size++;

                    // Add the msg to the acks and send it after we save the batch
                    dafka_proto_set_id (msg, DAFKA_PROTO_ACK);
                    dafka_proto_set_topic (msg, address);
                    dafka_proto_set_subject (msg, subject);
                    dafka_proto_set_sequence (msg, sequence);
                    s_put_head (acks, msg);

                    if (self->verbose)
                        zsys_info ("Store: Message added to batch %s %s %" PRIu64, subject, address, sequence);

                    // We stop the batch at 1000 messages
                    if (batch_size == 1000)
                        break;
                }
            }

            zframe_destroy (&content);
            zstr_free (&command);

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
            }

            // Batch is done, clearing all the acks
            zhashx_purge (acks);

            if (self->verbose && batch_size)
                zsys_info ("Store: Saved batch of %d", batch_size);
        }
    }

    leveldb_writeoptions_destroy (woptions);
    leveldb_readoptions_destroy (roptions);
    zhashx_destroy (&acks);
    dafka_proto_destroy (&msg);
    zpoller_destroy (&poller);
    zactor_destroy (&beacon);
    zsock_destroy (&publisher);
}

//  --------------------------------------------------------------------------
//  This is the actor which runs in its own thread.

void
dafka_store_actor (zsock_t *pipe, void *arg)
{
    dafka_store_t * self = dafka_store_new ((zconfig_t *) arg);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (pipe, 0);

    if (self->verbose)
        zsys_info ("Store: running...");

    bool terminated = false;

    zactor_t *writer = zactor_new ((zactor_fn *) dafka_store_write_actor, self);
    zactor_t *reader = zactor_new ((zactor_fn *) dafka_store_read_actor, self);
    zactor_t *beacon = zactor_new (dafka_beacon_actor, self->config);
    zsock_t *subscriber = zsock_new_sub (NULL, NULL);

    dafka_proto_subscribe (subscriber, DAFKA_PROTO_MSG, "");
    dafka_proto_subscribe (subscriber, DAFKA_PROTO_MSG, self->address);
    dafka_proto_subscribe (subscriber, DAFKA_PROTO_HEAD, "");
    dafka_proto_subscribe (subscriber, DAFKA_PROTO_FETCH, "");

    dafka_proto_t *msg = dafka_proto_new ();

    zpoller_t *poller = zpoller_new (beacon, pipe, subscriber, NULL);

    while (!terminated) {
        void *which = (zsock_t *) zpoller_wait (poller, -1);
        if (which == pipe) {
            char *command = zstr_recv (pipe);

            if (command == NULL)
                continue;

            if (streq (command, "$TERM"))
                terminated = true;

            zstr_free (&command);
        }
        if (which == beacon)
            dafka_beacon_recv (beacon, subscriber, self->verbose, "Store");
        if (which == subscriber) {
            int rc = dafka_proto_recv (msg, subscriber);
            if (rc == -1)
                continue;

            switch (dafka_proto_id (msg)) {
                case DAFKA_PROTO_FETCH: {
                    const char *sender = dafka_proto_address (msg);
                    const char *subject = dafka_proto_subject (msg);
                    const char *address = dafka_proto_topic (msg);
                    uint64_t sequence = dafka_proto_sequence (msg);
                    uint32_t count = dafka_proto_count (msg);

                    zstr_sendm (reader, "FETCH");
                    zsock_bsend (reader, "sss84", sender, subject, address, sequence, count);

                    break;
                }
                case DAFKA_PROTO_HEAD: {
                    const char *subject = dafka_proto_subject (msg);
                    const char *address = dafka_proto_address (msg);
                    uint64_t sequence = dafka_proto_sequence (msg);

                    zstr_sendm (writer, "HEAD");
                    zsock_bsend (writer, "ss8", subject, address, sequence);
                    break;
                }
                case DAFKA_PROTO_MSG: {
                    const char *subject = dafka_proto_subject (msg);
                    const char *address = dafka_proto_address (msg);
                    uint64_t sequence = dafka_proto_sequence (msg);
                    zframe_t *content = dafka_proto_get_content (msg);

                    zstr_sendm (writer, "MSG");
                    zsock_bsend (writer, "ss8p", subject, address, sequence, content);
                    break;
                }
                default:
                    assert (false);
            }
        }
    }

    zpoller_destroy (&poller);
    dafka_proto_destroy (&msg);
    zsock_destroy (&subscriber);
    zactor_destroy (&beacon);
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
    zactor_destroy (&store);
    zactor_destroy (&tower);
    zactor_destroy (&producer);
    zconfig_destroy (&config);
    //  @end

    printf ("OK\n");
}