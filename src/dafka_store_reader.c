/*  =========================================================================
    dafka_store_reader -

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
    dafka_store_reader -
@discuss
@end
*/

#include "dafka_classes.h"
#include <leveldb/c.h>

//  Structure of our actor

struct _dafka_store_reader_t {
    zsock_t *pipe;              //  Actor command pipe
    zpoller_t *poller;          //  Socket poller
    bool terminated;            //  Did caller ask us to quit?
    bool verbose;               //  Verbose logging enabled?

    leveldb_t *db;

    char* address;
    const char* writer_address;
    zsock_t *publisher;
    zsock_t *subscriber;
    zactor_t *beacon;

    dafka_proto_t *incoming_msg;
    dafka_proto_t *outgoing_msg;
    dafka_msg_key_t *first_key;
    dafka_msg_key_t *iter_key;
    dafka_msg_key_t *last_key;
    dafka_head_key_t *head_key;
};


//  --------------------------------------------------------------------------
//  Create a new dafka_store_reader instance

static dafka_store_reader_t *
dafka_store_reader_new (zsock_t *pipe, const char* writer_address, leveldb_t *db, zconfig_t *config)
{
    dafka_store_reader_t *self = (dafka_store_reader_t *) zmalloc (sizeof (dafka_store_reader_t));
    assert (self);

    self->pipe = pipe;
    self->terminated = false;

    if (atoi (zconfig_get (config, "store/verbose", "0")))
        self->verbose = true;

    self->db = db;
    self->writer_address = writer_address;

    //  Generating address for store reader
    self->address = generate_address ();

    //  Create and bind publisher
    self->publisher = zsock_new_xpub (NULL);
    zsock_set_xpub_verbose (self->publisher, 1);
    int port = zsock_bind (self->publisher, "tcp://*:*");

    //  Create the subscriber and subscribe for fetch & get heads messages
    self->subscriber = zsock_new_sub (NULL, NULL);
    dafka_proto_subscribe (self->subscriber, DAFKA_PROTO_FETCH, "");
    dafka_proto_subscribe (self->subscriber, DAFKA_PROTO_GET_HEADS, "");
    dafka_proto_subscribe (self->subscriber, DAFKA_PROTO_CONSUMER_HELLO, self->address);

    //  Create and start the beaconing
    dafka_beacon_args_t beacon_args = {"Store Reader", config};
    self->beacon = zactor_new (dafka_beacon_actor, &beacon_args);
    zsock_send (self->beacon, "ssi", "START", self->address, port);

    // Initialize the messages
    self->incoming_msg = dafka_proto_new ();
    self->outgoing_msg = dafka_proto_new ();
    self->first_key = dafka_msg_key_new ();
    self->iter_key = dafka_msg_key_new ();
    self->last_key = dafka_msg_key_new ();
    self->head_key = dafka_head_key_new ();

    self->poller = zpoller_new (self->pipe, self->subscriber, self->beacon, self->publisher, NULL);

    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_store_reader instance

static void
dafka_store_reader_destroy (dafka_store_reader_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_store_reader_t *self = *self_p;

        zpoller_destroy (&self->poller);
        dafka_head_key_destroy (&self->head_key);
        dafka_msg_key_destroy (&self->last_key);
        dafka_msg_key_destroy (&self->iter_key);
        dafka_msg_key_destroy (&self->first_key);
        dafka_proto_destroy (&self->incoming_msg);
        dafka_proto_destroy (&self->outgoing_msg);
        zactor_destroy (&self->beacon);
        zsock_destroy (&self->subscriber);
        zsock_destroy (&self->publisher);
        zstr_free (&self->address);
        //  Free object itself
        zpoller_destroy (&self->poller);
        free (self);
        *self_p = NULL;
    }
}


static void
s_send_heads (dafka_store_reader_t *self,
        leveldb_iterator_t *iter,
        const char *sender, const char *subject) {
    dafka_head_key_set (self->head_key, subject, "");
    dafka_head_key_iter_seek (self->head_key, iter);

    if (!leveldb_iter_valid (iter)) {
        if (self->verbose)
            zsys_info ("Store Reader: No heads for subject %s", subject);

        return;
    }

    dafka_proto_set_id (self->outgoing_msg, DAFKA_PROTO_DIRECT_HEAD);
    dafka_proto_set_topic (self->outgoing_msg, sender);

    int rc = dafka_head_key_iter (self->head_key, iter);
    while (rc == 0 && str_start_with (subject, dafka_head_key_subject (self->head_key))) {
        size_t sequence_size;
        const char* sequence_bytes = leveldb_iter_value (iter, &sequence_size);
        uint64_t sequence;
        uint64_get_be ((const byte*) sequence_bytes, &sequence);

        dafka_proto_set_subject (self->outgoing_msg, dafka_head_key_subject (self->head_key));
        dafka_proto_set_address (self->outgoing_msg, dafka_head_key_address (self->head_key));
        dafka_proto_set_sequence (self->outgoing_msg, sequence);
        dafka_proto_send (self->outgoing_msg, self->publisher);

        if (self->verbose)
            zsys_info ("Store Reader: Found head answer to consumer %s %s %lu",
                       dafka_head_key_subject (self->head_key),
                       dafka_head_key_address (self->head_key),
                       sequence);

        leveldb_iter_next (iter);
        if (!leveldb_iter_valid (iter))
            break;

        rc = dafka_head_key_iter (self->head_key, iter);
    }

}

//  Here we handle incoming message from the node

static void
dafka_store_reader_recv_api (dafka_store_reader_t *self)
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

static void
dafka_store_reader_recv_subscriber (dafka_store_reader_t *self) {
    int rc = dafka_proto_recv (self->incoming_msg, self->subscriber);
    if (rc == -1)
        return;

    const char *sender = dafka_proto_address (self->incoming_msg);

    // Ignore messages from the write actor
    if (streq (sender, self->writer_address))
        return;

    const leveldb_snapshot_t *snapshot = leveldb_create_snapshot (self->db);
    leveldb_readoptions_t *roptions = leveldb_readoptions_create ();
    leveldb_readoptions_set_snapshot (roptions, snapshot);
    leveldb_iterator_t *iter = leveldb_create_iterator (self->db, roptions);

    switch (dafka_proto_id (self->incoming_msg)) {
        case DAFKA_PROTO_FETCH: {

            const char *subject = dafka_proto_subject (self->incoming_msg);;
            const char *address = dafka_proto_topic (self->incoming_msg);
            uint64_t sequence = dafka_proto_sequence (self->incoming_msg);
            uint32_t count = dafka_proto_count (self->incoming_msg);

            //  Search the first message
            dafka_msg_key_set (self->first_key, subject, address, sequence);
            dafka_msg_key_iter_seek (self->first_key, iter);

            if (!leveldb_iter_valid (iter)) {
                if (self->verbose)
                    zsys_info ("Store: no answer for consumer. Subject: %s, Address: %s, Seq: %" PRIu64,
                               subject, address, sequence);

                leveldb_iter_destroy (iter);
                leveldb_readoptions_destroy (roptions);
                leveldb_release_snapshot (self->db, snapshot);

                return;
            }

            // For now we only sending from what the user requested, so if the sequence at the
            // beginning doesn't match, don't send anything
            // TODO: mark the first message as tail
            dafka_msg_key_iter (self->iter_key, iter);
            if (dafka_msg_key_cmp (self->first_key, self->iter_key) != 0) {
                if (self->verbose)
                    zsys_info ("Store: no answer for consumer. Subject: %s, Address: %s, Seq: %" PRIu64,
                               subject, address, sequence);

                leveldb_iter_destroy (iter);
                leveldb_readoptions_destroy (roptions);
                leveldb_release_snapshot (self->db, snapshot);

                return;
            }

            dafka_msg_key_set (self->last_key, subject, address, sequence + count);

            dafka_proto_set_topic (self->outgoing_msg, sender);
            dafka_proto_set_subject (self->outgoing_msg, subject);
            dafka_proto_set_address (self->outgoing_msg, address);
            dafka_proto_set_id (self->outgoing_msg, DAFKA_PROTO_DIRECT_RECORD);

            while (dafka_msg_key_cmp (self->iter_key, self->last_key) <= 0) {
                size_t content_size;
                const char *content = leveldb_iter_value (iter, &content_size);
                zframe_t *frame = zframe_new (content, content_size);

                uint64_t iter_sequence = dafka_msg_key_sequence (self->iter_key);

                dafka_proto_set_sequence (self->outgoing_msg, iter_sequence);
                dafka_proto_set_content (self->outgoing_msg, &frame);
                dafka_proto_send (self->outgoing_msg, self->publisher);

                if (self->verbose)
                    zsys_info ("Store: found answer for consumer. Subject: %s, Partition: %s, Seq: %lu",
                               subject, address, iter_sequence);

                leveldb_iter_next (iter);

                if (!leveldb_iter_valid (iter))
                    break;

                //  Get the next key, if it not a valid key (different table), break the loop
                if (dafka_msg_key_iter (self->iter_key, iter) == -1)
                    break;
            }
            break;
        }
        case DAFKA_PROTO_CONSUMER_HELLO: {
            zlist_t *subjects = dafka_proto_get_subjects (self->incoming_msg);

            for (const char* subject = (const char *) zlist_first (subjects);
                 subject != NULL;
                 subject = (const char *) zlist_next (subjects)) {
                s_send_heads (self, iter, sender, subject);
            }

            zlist_destroy (&subjects);

            break;
        }
        case DAFKA_PROTO_GET_HEADS: {
            const char *subject = dafka_proto_topic (self->incoming_msg);
            s_send_heads (self, iter, sender, subject);

            break;
        }
        default:
            assert (false);
    }

    leveldb_iter_destroy (iter);
    leveldb_readoptions_destroy (roptions);
    leveldb_release_snapshot (self->db, snapshot);
}

static void
dafka_store_reader_recv_publisher (dafka_store_reader_t *self) {
    int rc = dafka_proto_recv (self->incoming_msg, self->publisher);
    if (rc == -1)
        return;

    const char *consumer_address = dafka_proto_topic (self->incoming_msg);

    if (dafka_proto_id (self->incoming_msg) == DAFKA_PROTO_STORE_HELLO && dafka_proto_is_subscribe (self->incoming_msg)) {
        if (self->verbose)
            zsys_info ("Store Reader: Consumer %s connected", consumer_address);

        dafka_proto_set_id (self->outgoing_msg, DAFKA_PROTO_STORE_HELLO);
        dafka_proto_set_address (self->outgoing_msg, self->address);
        dafka_proto_set_topic (self->outgoing_msg, consumer_address);
        dafka_proto_send (self->outgoing_msg, self->publisher);
    }
}

//  --------------------------------------------------------------------------
//  This is the actor which runs in its own thread.

void
dafka_store_reader_actor (zsock_t *pipe, dafka_store_reader_args_t *args)
{
    dafka_store_reader_t * self = dafka_store_reader_new (pipe, args->writer_address, args->db, args->config);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (self->pipe, 0);

    while (!self->terminated) {
        void *which = (zsock_t *) zpoller_wait (self->poller, -1);
        if (which == self->pipe)
            dafka_store_reader_recv_api (self);
        if (which == self->beacon)
            dafka_beacon_recv (self->beacon, self->subscriber, self->verbose, "Store Reader");
        if (which == self->subscriber)
            dafka_store_reader_recv_subscriber (self);
        if (which == self->publisher)
            dafka_store_reader_recv_publisher (self);
    }
    dafka_store_reader_destroy (&self);
}
