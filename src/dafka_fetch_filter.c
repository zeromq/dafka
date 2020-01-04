/*  =========================================================================
    dafka_fetch_filter - class description

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
    dafka_fetch_filter -
@discuss
@end
*/

#include "dafka_classes.h"

//  Structure of our class

typedef struct {
    char subject[256];
    char address[256];
    uint64_t sequence_hash;
    uint64_t time_hash;
} fetch_request_t;

struct _dafka_fetch_filter_t {
    zsock_t *publisher;
    bool verbose;
    int filter_size;
    fetch_request_t *cache;
    dafka_proto_t *msg;
};


//  --------------------------------------------------------------------------
//  Create a new dafka_fetch_filter

dafka_fetch_filter_t *
dafka_fetch_filter_new (zsock_t *publisher, const char *address, bool verbose)
{
    dafka_fetch_filter_t *self = (dafka_fetch_filter_t *) zmalloc (sizeof (dafka_fetch_filter_t));
    assert (self);

    self->publisher = publisher;
    self->filter_size = 10000;
    self->cache = (fetch_request_t *) zmalloc (sizeof (fetch_request_t) * self->filter_size);
    self->msg = dafka_proto_new ();
    self->verbose = verbose;

    dafka_proto_set_id (self->msg, DAFKA_PROTO_FETCH);
    dafka_proto_set_address (self->msg, address);


    //  Initialize class properties here
    return self;
}

static uint64_t
hash_string (const char *s)
{
    uint64_t hash = 0;
    while (*s != '\0')
        hash = 33 * hash ^ *s++;

    return hash;
}

void
dafka_fetch_filter_send (dafka_fetch_filter_t *self, const char *subject, const char *address, uint64_t sequence)
{
    int64_t time = zclock_mono ();
    int max_count = 100000;

    uint64_t sequence_hash = sequence / max_count;
    uint64_t time_hash = (time / 1000);
    int count = (sequence_hash + 1) * max_count - sequence;

    uint64_t hash = hash_string(subject);
    hash = 33 * hash ^ hash_string(address);
    hash = 33 * hash ^ sequence_hash;
    hash = 33 * hash ^ time_hash;
    int index = hash % self->filter_size;

    fetch_request_t *request = &self->cache[index];

    if (sequence_hash == request->sequence_hash &&
        time_hash == request->time_hash &&
        streq (subject, request->subject) &&
        streq (address, request->address)) {
        if (self->verbose)
            zsys_debug ("Filter: Filtered fetch request %s %s %" PRIu64, subject, address, sequence);
    } else {
        if (self->verbose)
            zsys_debug ("Filter: Sending fetch request %s %s %" PRIu64, subject, address, sequence);

        strncpy (request->subject, subject, 255);
        strncpy (request->address, address, 255);
        request->sequence_hash = sequence_hash;
        request->time_hash = time_hash;

        dafka_proto_set_topic (self->msg, address);
        dafka_proto_set_subject (self->msg, subject);
        dafka_proto_set_sequence (self->msg, sequence);
        dafka_proto_set_count (self->msg, count);

        dafka_proto_send (self->msg, self->publisher);
    }
}

//  --------------------------------------------------------------------------
//  Destroy the dafka_fetch_filter

void
dafka_fetch_filter_destroy (dafka_fetch_filter_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_fetch_filter_t *self = *self_p;
        //  Free class properties here
        free (self->cache);
        self->cache = NULL;
        dafka_proto_destroy (&self->msg);
        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}