/*  =========================================================================
    dafka_unacked_list - class description

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
    dafka_unacked_list -
@discuss
@end
*/

#include "dafka_classes.h"

//  Structure of our class

#define CHUNK_SIZE 256

typedef struct _chunk_t {
    zmq_msg_t values[CHUNK_SIZE];
    struct _chunk_t *next;
    uint64_t begin_seq;
    uint64_t count;
} chunk_t;

struct _dafka_unacked_list_t {
    chunk_t *begin_chunk;
    chunk_t *end_chunk;

    chunk_t *spare_chunk;

    zsock_t *publisher;
    dafka_proto_t *direct_msg;
};

static chunk_t*
chunk_new (uint64_t begin_seq) {
    chunk_t *chunk = (chunk_t *) malloc (sizeof (chunk_t));
    for (int i = 0; i < CHUNK_SIZE; ++i) {
        zmq_msg_init (&chunk->values[i]);
    }
    chunk->next = NULL;
    chunk->begin_seq = begin_seq;
    chunk->count = 0;

    return chunk;
}

static void chunk_destroy (chunk_t **self_p) {
    assert (self_p);
    if (*self_p) {
        chunk_t *self = *self_p;
        //  Free class properties here
        for (int i = 0; i < CHUNK_SIZE; ++i)
            zmq_msg_close (&self->values[i]);

        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}

//  --------------------------------------------------------------------------
//  Create a new dafka_unacked_list

dafka_unacked_list_t *
dafka_unacked_list_new (zsock_t *publisher, const char* address, const char *topic)
{
    dafka_unacked_list_t *self = (dafka_unacked_list_t *) zmalloc (sizeof (dafka_unacked_list_t));
    assert (self);

    //  Initialize class properties here
    self->begin_chunk = chunk_new (0);
    self->end_chunk = self->begin_chunk;
    self->publisher = publisher;
    self->direct_msg = dafka_proto_new ();

    dafka_proto_set_id (self->direct_msg, DAFKA_PROTO_DIRECT_RECORD);
    dafka_proto_set_subject (self->direct_msg, topic);
    dafka_proto_set_address (self->direct_msg, address);

    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_unacked_list

void
dafka_unacked_list_destroy (dafka_unacked_list_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_unacked_list_t *self = *self_p;
        //  Free class properties here
        dafka_proto_destroy (&self->direct_msg);
        chunk_destroy (&self->begin_chunk);
        chunk_destroy (&self->spare_chunk);

        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}


uint64_t
dafka_unacked_list_push (dafka_unacked_list_t *self, zmq_msg_t *msg)
{
    zmq_msg_copy (&self->end_chunk->values[self->end_chunk->count], msg);
    uint64_t sequence = self->end_chunk->begin_seq + self->end_chunk->count;
    self->end_chunk->count++;

    if (self->end_chunk->count == CHUNK_SIZE) {
        chunk_t *spare_chunk = self->spare_chunk;
        self->spare_chunk = NULL;
        if (spare_chunk) {
            self->end_chunk->next = spare_chunk;
            self->end_chunk->next->count = 0;
            self->end_chunk->next->begin_seq = sequence + 1;
        }
        else
            self->end_chunk->next = chunk_new (sequence + 1);

        self->end_chunk = self->end_chunk->next;
    }

    return sequence;
}

void
dafka_unacked_list_ack (dafka_unacked_list_t *self, uint64_t sequence)
{
    while (sequence >= self->begin_chunk->begin_seq) {
        uint64_t last_sequence = self->begin_chunk->begin_seq + self->begin_chunk->count;

        if (sequence < last_sequence) {
            uint64_t begin_sequence = sequence + 1;
            uint64_t count = last_sequence - begin_sequence;
            self->begin_chunk->begin_seq = begin_sequence;
            self->begin_chunk->count = count;
            return;
        }
        else {
            // Is it the last chunk? lets re-use it
            if (self->begin_chunk->next == NULL) {
                self->begin_chunk->begin_seq = self->begin_chunk->begin_seq + self->begin_chunk->count;
                self->begin_chunk->count = 0;

                return;
            } else {
                chunk_t *temp = self->begin_chunk;
                self->begin_chunk = self->begin_chunk->next;

                if (self->spare_chunk == NULL) {
                    self->spare_chunk = temp;
                }
                else
                    chunk_destroy (&temp);
            }
        }
    }

}

void
dafka_unacked_list_send (dafka_unacked_list_t *self, const char* address, uint64_t sequence, uint32_t count)
{
    if (sequence < self->begin_chunk->begin_seq)
        return;

    dafka_proto_set_topic (self->direct_msg, address);
    chunk_t *current = self->begin_chunk;

    zmq_msg_t msg;
    zmq_msg_init (&msg);

    while (current && count > 0) {
        if (sequence >= current->begin_seq && sequence < current->begin_seq + current->count) {
            int rc = zmq_msg_copy (&msg, &self->begin_chunk->values[sequence - current->begin_seq]);
            assert (rc == 0);

            dafka_proto_set_sequence(self->direct_msg, sequence);
            dafka_proto_set_content(self->direct_msg, &msg);
            dafka_proto_send (self->direct_msg,  self->publisher);

            count--;
            sequence++;
        } else
            current = current->next;
    }

    zmq_msg_close (&msg);
}

bool
dafka_unacked_list_is_empty (dafka_unacked_list_t *self)
{
    return self->begin_chunk == self->end_chunk && self->begin_chunk->count == 0;
}

uint64_t
dafka_unacked_list_last_acked (dafka_unacked_list_t *self) {
    return self->begin_chunk->begin_seq - 1;
}

//  --------------------------------------------------------------------------
//  Self test of this class

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
dafka_unacked_list_test (bool verbose)
{
    printf (" * dafka_unacked_list: ");

    //  @selftest
    //  Simple create/destroy test

    const char* subAddress = "SUBADDRESS";

    zsock_t *publisher = zsock_new_pub("inproc://unacked-list-selftest");
    zsock_set_sndhwm (publisher, 0);
    zsock_t *subscriber = zsock_new_sub("inproc://unacked-list-selftest", "");
    zsock_set_rcvhwm (subscriber, 0);
    zsock_set_rcvtimeo (subscriber, 100);

    dafka_unacked_list_t *self = dafka_unacked_list_new (publisher, "ADDRESS", subAddress);
    assert (self);

    // Storing messages
    for (int i = 0; i < 1000000; ++i) {
        zmq_msg_t msg;
        zmq_msg_init_size(&msg, 1000);
        dafka_unacked_list_push (self, &msg);
        zmq_msg_close (&msg);
    }

    // Checks that the list is not empty
    assert (dafka_unacked_list_is_empty (self) == false);

    // Receive the two requested messages
    dafka_unacked_list_send (self, subAddress, 0, 2);
    dafka_proto_t *proto = dafka_proto_new ();
    int rc = dafka_proto_recv (proto, subscriber);
    assert (rc == 0);
    assert (dafka_proto_sequence (proto) == 0);
    assert (zmq_msg_size (dafka_proto_content (proto)) == 1000);
    rc = dafka_proto_recv (proto, subscriber);
    assert (rc == 0);
    assert (dafka_proto_sequence (proto) == 1);
    assert (zmq_msg_size (dafka_proto_content (proto)) == 1000);

    // Ack some messages
    dafka_unacked_list_ack (self, 100000);
    assert (dafka_unacked_list_is_empty (self) == false);

    // Try to send messages that was already acked, nothing should be sent
    dafka_unacked_list_send (self, subAddress, 0, 2);
    rc = dafka_proto_recv (proto, subscriber);
    assert (rc == -1 && errno == EAGAIN);

    // Try to send messages that was not acked yet, should work
    dafka_unacked_list_send (self, subAddress, 100001, 2);
    rc = dafka_proto_recv (proto, subscriber);
    assert (rc == 0);
    assert (dafka_proto_sequence (proto) == 100001);
    assert (zmq_msg_size (dafka_proto_content (proto)) == 1000);
    rc = dafka_proto_recv (proto, subscriber);
    assert (rc == 0);
    assert (dafka_proto_sequence (proto) == 100002);
    assert (zmq_msg_size (dafka_proto_content (proto)) == 1000);

    // Asking to send sequence which doesn't exist
    dafka_unacked_list_send (self, subAddress, 999999, 2);
    rc = dafka_proto_recv (proto, subscriber);
    assert (rc == 0);
    assert (dafka_proto_sequence (proto) == 999999);
    assert (zmq_msg_size (dafka_proto_content (proto)) == 1000);
    rc = dafka_proto_recv (proto, subscriber);
    assert (rc == -1 && errno == EAGAIN);

    // Acking more messages
    dafka_unacked_list_ack (self, 200000);
    assert (dafka_unacked_list_is_empty (self) == false);
    assert (dafka_unacked_list_last_acked (self) == 200000);
    dafka_unacked_list_ack (self, 304000);
    assert (dafka_unacked_list_is_empty (self) == false);
    assert (dafka_unacked_list_last_acked (self) == 304000);
    dafka_unacked_list_ack (self, 400073);
    assert (dafka_unacked_list_is_empty (self) == false);
    assert (dafka_unacked_list_last_acked (self) == 400073);
    dafka_unacked_list_ack (self, 586321);
    assert (dafka_unacked_list_is_empty (self) == false);
    assert (dafka_unacked_list_last_acked (self) == 586321);

    // Storing some more
    for (int i = 0; i < 100000; ++i) {
        zmq_msg_t msg;
        zmq_msg_init_size(&msg, 1000);
        dafka_unacked_list_push (self, &msg);
        zmq_msg_close (&msg);
    }

    // Acking the rest
    dafka_unacked_list_ack (self, 874321);
    assert (dafka_unacked_list_is_empty (self) == false);
    assert (dafka_unacked_list_last_acked (self) == 874321);
    dafka_unacked_list_ack (self, 1099999);
    assert (dafka_unacked_list_is_empty (self));
    assert (dafka_unacked_list_last_acked (self) == 1099999);

    // Storing some more
    for (int i = 0; i < 100000; ++i) {
        zmq_msg_t msg;
        zmq_msg_init_size(&msg, 1000);
        dafka_unacked_list_push (self, &msg);
        zmq_msg_close (&msg);
    }

    assert (dafka_unacked_list_is_empty (self) == false);

    // Asking for all messages
    dafka_unacked_list_send (self, subAddress, 1100000, 100000);
    for (int i = 0; i < 100000; ++i) {
        rc = dafka_proto_recv (proto, subscriber);
        assert (rc == 0);
        assert (dafka_proto_sequence (proto) == 1100000 + i);
        assert (zmq_msg_size (dafka_proto_content (proto)) == 1000);
    }

    // Acking the rest
    dafka_unacked_list_ack (self, 1199999);
    assert (dafka_unacked_list_is_empty (self));
    assert (dafka_unacked_list_last_acked (self) == 1199999);

    // Ack a sequence which doesn't exist
    dafka_unacked_list_ack (self, 2000000);
    assert (dafka_unacked_list_is_empty (self));

    zsock_destroy (&publisher);
    zsock_destroy(&subscriber);
    dafka_proto_destroy (&proto);
    dafka_unacked_list_destroy (&self);
    //  @end
    printf ("OK\n");
}
