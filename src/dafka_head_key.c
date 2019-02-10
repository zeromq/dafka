/*  =========================================================================
    dafka_head_key - class description

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
    dafka_head_key -
@discuss
@end
*/

#include "dafka_classes.h"

#define MAX_HEAD_KEY_SIZE 1 + 256 + 256

//  Structure of our class

struct _dafka_head_key_t {
    byte buffer[MAX_HEAD_KEY_SIZE];
    size_t buffer_size;
    const char *subject;
    const char *address;

    uint32_t hash;
};


//  --------------------------------------------------------------------------
//  Create a new dafka_head_key

dafka_head_key_t *
dafka_head_key_new (void)
{
    dafka_head_key_t *self = (dafka_head_key_t *) zmalloc (sizeof (dafka_head_key_t));
    assert (self);
    //  Initialize class properties here
    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_head_key

void
dafka_head_key_destroy (dafka_head_key_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_head_key_t *self = *self_p;
        //  Free class properties here
        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}

void
dafka_head_key_set (dafka_head_key_t *self, const char* subject, const char* address) {
    assert (self);

    size_t subject_len = strlen (subject);
    size_t address_len = strlen (address);

    byte *needle = self->buffer;
    *needle = 'H';
    needle++;

    memcpy (needle, subject, subject_len + 1);
    self->subject = (const char *) needle;
    needle += subject_len + 1;

    memcpy (needle, address, address_len + 1);
    self->address = (const char *) needle;
    needle += address_len + 1;

    self->buffer_size = needle - self->buffer;

    self->hash = 0;
    const byte *pointer = (const byte *) self->buffer;
    while (pointer != needle)
        self->hash = 33 * self->hash ^ *pointer++;
}

const char *
dafka_head_key_subject (dafka_head_key_t *self) {
    assert (self);
    return self->subject;
}

const char *
dafka_head_key_address (dafka_head_key_t *self) {
    assert (self);
    return self->address;
}

dafka_head_key_t *
dafka_head_key_dup (dafka_head_key_t *self) {
    assert (self);

    dafka_head_key_t *copy = dafka_head_key_new ();
    dafka_head_key_decode (copy, self->buffer, self->buffer_size);

    return copy;
}

uint32_t
dafka_head_key_hash (dafka_head_key_t *self) {
    assert (self);
    return self->hash;
}

// Encode head key as bytes for leveldb
const char *
dafka_head_key_encode (dafka_head_key_t *self, size_t *size_p) {
    assert (self);

    *size_p = self->buffer_size;

    return (const char *) self->buffer;
}

int
dafka_head_key_decode (dafka_head_key_t *self, byte* buffer, size_t size) {
    assert (self);

    if (size < 1 + 2)
        return -1;

    if (*buffer != 'H')
        return -1;

    memcpy (self->buffer, buffer, size);
    byte* needle = self->buffer;
    needle++;

    self->subject = (const char *) needle;
    needle += strlen (self->subject) + 1;

    self->address = (const char *) needle;
    needle += strlen (self->address) + 1;

    self->buffer_size = needle - self->buffer;
    assert (self->buffer_size == size);

    self->hash = 0;
    return 0;
}

int
dafka_head_key_cmp (const dafka_head_key_t *self, const dafka_head_key_t *other) {
    const size_t min_size = (self->buffer_size < other->buffer_size) ? self->buffer_size : other->buffer_size;
    int r = memcmp(self->buffer, other->buffer, min_size);
    if (r == 0) {
        if (self->buffer_size < other->buffer_size)
            r = -1;
        else
        if (self->buffer_size > other->buffer_size)
            r = 1;
    }
    return r;
}

// Set hashx key functions, comparator, hasher, dup and destroy
void
dafka_head_key_hashx_set (zhashx_t *hashx) {
    zhashx_set_key_destructor (hashx, (zhashx_destructor_fn *) dafka_head_key_destroy);
    zhashx_set_key_comparator (hashx, (zhashx_comparator_fn *) dafka_head_key_cmp);
    zhashx_set_key_hasher (hashx, (zhashx_hash_fn *) dafka_head_key_hash);
    zhashx_set_key_duplicator (hashx, (zhashx_duplicator_fn *) dafka_head_key_dup);
}
