/*  =========================================================================
    dafka_store_writer -

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
    dafka_store_writer -
@discuss
@end
*/

#include "dafka_classes.h"

#define NULL_SEQUENCE  0xFFFFFFFFFFFFFFFF

//  Structure of our actor

struct _dafka_store_writer_t {
    zsock_t *pipe;              //  Actor command pipe
    zpoller_t *poller;          //  Socket poller
    bool terminated;            //  Did caller ask us to quit?
    bool verbose;               //  Verbose logging enabled?

    const char *address;

    leveldb_t *db;
    leveldb_writeoptions_t *woptions;
    leveldb_readoptions_t *roptions;

    zsock_t *publisher;
    zsock_t *direct_subscriber;
    zsock_t *msg_subscriber;
    zactor_t *beacon;

    dafka_proto_t *incoming_msg;
    dafka_proto_t *outgoing_msg;

    dafka_head_key_t *head_key;
    dafka_msg_key_t *msg_key;

    zhashx_t *heads;

    dafka_fetch_filter_t *fetch_filter;
};


//  --------------------------------------------------------------------------
//  Create a new dafka_store_writer instance

static dafka_store_writer_t *
dafka_store_writer_new (zsock_t *pipe, const char* address, leveldb_t *db, zconfig_t *config)
{
    dafka_store_writer_t *self = (dafka_store_writer_t *) zmalloc (sizeof (dafka_store_writer_t));
    assert (self);

    self->pipe = pipe;
    self->terminated = false;

    if (atoi (zconfig_get (config, "store/verbose", "0")))
        self->verbose = true;

    self->address = address;

    self->db = db;
    self->roptions = leveldb_readoptions_create ();
    self->woptions = leveldb_writeoptions_create ();

    //  Create and bind publisher
    self->publisher = zsock_new_pub (NULL);
    int port = zsock_bind (self->publisher, "tcp://*:*");

    int hwm = atoi (zconfig_get (config, "store/high_watermark", "1000000"));

    // We are using different subscribers for DIRECT and MSG in order to
    // give the DIRECT messages an higher priority

    // Create the direct subscriber and subscribe to direct messaging
    self->direct_subscriber = zsock_new_sub (NULL, NULL);
    zsock_set_rcvtimeo(self->direct_subscriber, 0);
    zsock_set_rcvhwm (self->direct_subscriber, hwm);
    dafka_proto_subscribe (self->direct_subscriber, DAFKA_PROTO_DIRECT_RECORD, self->address);

    //  Create the msg subscriber and subscribe to msg and head
    self->msg_subscriber = zsock_new_sub (NULL, NULL);
    zsock_set_rcvtimeo(self->msg_subscriber, 0);
    zsock_set_rcvhwm (self->msg_subscriber, hwm);
    dafka_proto_subscribe (self->msg_subscriber, DAFKA_PROTO_RECORD, "");
    dafka_proto_subscribe (self->msg_subscriber, DAFKA_PROTO_HEAD, "");


    //  Create and start the beaconing
    dafka_beacon_args_t beacon_args = {"Store Writer", config};
    self->beacon = zactor_new (dafka_beacon_actor, &beacon_args);
    zsock_send (self->beacon, "ssi", "START", self->address, port);

    //  Create the messages
    self->incoming_msg = dafka_proto_new ();
    self->msg_key = dafka_msg_key_new ();
    self->head_key = dafka_head_key_new ();

    // Create cache of the heads
    self->heads = zhashx_new ();
    dafka_head_key_hashx_set (self->heads);
    zhashx_set_destructor (self->heads, uint64_destroy);
    zhashx_set_duplicator (self->heads, uint64_dup);

    // Create the fetch filter
    self->fetch_filter = dafka_fetch_filter_new (self->publisher, self->address, self->verbose);

    self->poller = zpoller_new (self->direct_subscriber,
            self->msg_subscriber, self->pipe, self->beacon, self->publisher, NULL);

    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_store_writer instance

static void
dafka_store_writer_destroy (dafka_store_writer_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_store_writer_t *self = *self_p;

        dafka_fetch_filter_destroy (&self->fetch_filter);
        zhashx_destroy (&self->heads);
        dafka_msg_key_destroy (&self->msg_key);
        dafka_head_key_destroy (&self->head_key);
        zpoller_destroy (&self->poller);
        dafka_proto_destroy (&self->incoming_msg);
        dafka_proto_destroy (&self->outgoing_msg);
        zactor_destroy (&self->beacon);
        zsock_destroy (&self->msg_subscriber);
        zsock_destroy (&self->direct_subscriber);
        zsock_destroy (&self->publisher);
        leveldb_writeoptions_destroy (self->woptions);
        leveldb_readoptions_destroy (self->roptions);

        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}

static uint64_t
dafka_store_write_get_head (dafka_store_writer_t *self) {
    uint64_t *sequence_p = (uint64_t *) zhashx_lookup (self->heads, self->head_key);
    if (sequence_p)
        return *sequence_p;

    size_t key_size;
    const char* key_bytes = dafka_head_key_encode (self->head_key, &key_size);

    char *err = NULL;
    size_t value_size;
    char* value_bytes = leveldb_get (self->db, self->roptions, key_bytes, key_size, &value_size, &err);
    if (err) {
        zsys_error ("Store Writer: failed to get head from db %s", err);
        assert (false);
    }

    if (value_bytes == NULL)
        return NULL_SEQUENCE;

    assert (value_size == 8);
    uint64_t sequence;
    uint64_get_be ((const byte *)value_bytes, &sequence);
    leveldb_free (value_bytes);

    return sequence;
}

//  Here we handle incoming message from the node

static void
dafka_store_writer_recv_api (dafka_store_writer_t *self)
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

static void dafka_store_writer_add_ack (dafka_store_writer_t *self,
                                          zhashx_t *acks,
                                          uint64_t sequence) {
    dafka_proto_t *msg = (dafka_proto_t *) zhashx_lookup (acks, self->head_key);

    if (msg == NULL) {
        msg = dafka_proto_new ();

        dafka_proto_set_id (msg, DAFKA_PROTO_ACK);
        dafka_proto_set_topic (msg, dafka_head_key_address (self->head_key));
        dafka_proto_set_subject (msg, dafka_head_key_subject (self->head_key));
        zhashx_insert (acks, self->head_key, msg);
    }

    dafka_proto_set_sequence (msg, sequence);
}


static void
dafka_store_writer_recv_subscriber (dafka_store_writer_t *self) {
    int batch_size = 0;
    leveldb_writebatch_t *batch = leveldb_writebatch_create ();

    zhashx_t *acks = zhashx_new ();
    dafka_head_key_hashx_set (acks);
    zhashx_set_duplicator (acks, NULL);
    zhashx_set_destructor (acks, (zhashx_destructor_fn *) dafka_proto_destroy);

    while (batch_size < 100000) {

        // We always check the DIRECT subscriber first, this is how we gave it a priority over the MSG subscriber
        int rc = dafka_proto_recv (self->incoming_msg, self->direct_subscriber);

        if (rc == -1 && errno == EAGAIN)
            rc = dafka_proto_recv (self->incoming_msg, self->msg_subscriber);

        if (rc == -1)
            break;

        switch (dafka_proto_id (self->incoming_msg)) {
            case DAFKA_PROTO_HEAD: {
                const char *subject = dafka_proto_subject (self->incoming_msg);
                const char *address = dafka_proto_address (self->incoming_msg);
                uint64_t sequence = dafka_proto_sequence (self->incoming_msg);

                dafka_head_key_set (self->head_key, subject, address);

                uint64_t head = dafka_store_write_get_head (self);

                if (head == NULL_SEQUENCE) {
                    dafka_fetch_filter_send (self->fetch_filter, subject, address, 0);
                }
                else
                if (head < sequence) {
                    dafka_fetch_filter_send (self->fetch_filter, subject, address, head + 1);
                }
                break;
            }
            case DAFKA_PROTO_DIRECT_RECORD:
            case DAFKA_PROTO_RECORD: {
                const char *subject = dafka_proto_subject (self->incoming_msg);
                const char *address = dafka_proto_address (self->incoming_msg);
                uint64_t sequence = dafka_proto_sequence (self->incoming_msg);
                zmq_msg_t *content = dafka_proto_content (self->incoming_msg);

                dafka_head_key_set (self->head_key, subject, address);
                uint64_t head = dafka_store_write_get_head (self);

                if (head != NULL_SEQUENCE && sequence <= head) {
                    if (self->verbose)
                        zsys_debug ("Store: head at % "PRIu64 " dropping already received message %s %s %" PRIu64,
                                    head, subject, address, sequence);
                }
                else
                if (head == NULL_SEQUENCE && sequence != 0) {
                    dafka_fetch_filter_send (self->fetch_filter, subject, address, 0);
                }
                else
                if (head != NULL_SEQUENCE && head + 1 != sequence) {
                    dafka_fetch_filter_send (self->fetch_filter, subject, address, head + 1);
                }
                else {
                    // Saving msg to db
                    dafka_msg_key_set (self->msg_key, subject, address, sequence);
                    size_t msg_key_size;
                    const char *msg_key_bytes = dafka_msg_key_encode (self->msg_key, &msg_key_size);
                    leveldb_writebatch_put (batch,
                                            msg_key_bytes, msg_key_size,
                                            (const char *) zmq_msg_data (content), zmq_msg_size (content));

                    // update the head
                    char sequence_bytes[8];
                    size_t sequence_size = uint64_put_be ((byte *) sequence_bytes, sequence);
                    size_t head_key_size;
                    const char *head_key_bytes = dafka_head_key_encode (self->head_key, &head_key_size);
                    leveldb_writebatch_put (batch,
                                            head_key_bytes, head_key_size,
                                            sequence_bytes, sequence_size); // TODO; we might be override it multiple times in a batch
                    batch_size++;

                    // Add the ack message
                    dafka_store_writer_add_ack (self, acks, sequence);

                    // Update the head in the heads hash
                    zhashx_update (self->heads, self->head_key, &sequence);
                }

                break;
            }
            default:
                break;
        }
    }

    char *err = NULL;
    leveldb_write (self->db, self->woptions, batch, &err);
    if (err) {
        zsys_error ("Store: failed to save batch to db %s", err);
        assert (false);
    }
    leveldb_writebatch_destroy (batch);

    // Sending the acks now
    for (dafka_proto_t *ack_msg = (dafka_proto_t *) zhashx_first (acks);
         ack_msg != NULL;
         ack_msg = (dafka_proto_t *) zhashx_next (acks)) {
        dafka_proto_send (ack_msg, self->publisher);

        if (self->verbose)
            zsys_info ("Store: Acked %s %s %" PRIu64,
                       dafka_proto_subject (ack_msg),
                       dafka_proto_topic (ack_msg),
                       dafka_proto_sequence (ack_msg));
    }

    // Batch is done, clearing all the acks and fetches
    zhashx_destroy (&acks);

    if (self->verbose)
        zsys_info ("Store: Saved batch of %d", batch_size);
}

static void
dafka_store_writer_recv_beacon (dafka_store_writer_t *self) {
    char *command = zstr_recv (self->beacon);
    if (command == NULL)
        return;

    char *address = zstr_recv (self->beacon);

    if (streq (command, "CONNECT")) {
        if (self->verbose)
            zsys_info ("Store: Connecting to %s", address);

        zsock_connect (self->direct_subscriber, "%s", address);
        zsock_connect (self->msg_subscriber, "%s", address);
    }
    else
    if (streq (command, "DISCONNECT")) {
        zsock_disconnect (self->direct_subscriber, "%s", address);
        zsock_disconnect (self->msg_subscriber, "%s", address);
    }
    else {
        zsys_error ("Transport: Unknown command %s", command);
        assert (false);
    }

    zstr_free (&address);
    zstr_free (&command);
}


//  --------------------------------------------------------------------------
//  This is the actor which runs in its own thread.

void
dafka_store_writer_actor (zsock_t *pipe, dafka_store_writer_args_t *args)
{
    dafka_store_writer_t * self = dafka_store_writer_new (pipe, args->address, args->db, args->config);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (self->pipe, 0);

    while (!self->terminated) {
        void *which = (zsock_t *) zpoller_wait (self->poller, -1);
        if (which == self->pipe)
            dafka_store_writer_recv_api (self);
        if (which == self->direct_subscriber || which == self->msg_subscriber)
            dafka_store_writer_recv_subscriber (self);
        if (which == self->beacon)
            dafka_store_writer_recv_beacon (self);
    }
    dafka_store_writer_destroy (&self);
}