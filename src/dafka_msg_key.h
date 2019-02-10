/*  =========================================================================
    dafka_msg_key - class description

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_MSG_KEY_H_INCLUDED
#define DAFKA_MSG_KEY_H_INCLUDED

#include <leveldb/c.h>

#ifdef __cplusplus
extern "C" {
#endif

//  @interface
//  Create a new dafka_msg_key
DAFKA_PRIVATE dafka_msg_key_t *
    dafka_msg_key_new (void);

//  Destroy the dafka_msg_key
DAFKA_PRIVATE void
    dafka_msg_key_destroy (dafka_msg_key_t **self_p);

DAFKA_PRIVATE void
    dafka_msg_key_set (dafka_msg_key_t *self, const char* subject, const char* address, uint64_t sequence);

DAFKA_PRIVATE const char *
    dafka_msg_key_subject (dafka_msg_key_t *self);

DAFKA_PRIVATE const char *
    dafka_msg_key_address (dafka_msg_key_t *self);

DAFKA_PRIVATE uint64_t
    dafka_msg_key_sequence (dafka_msg_key_t *self);

DAFKA_PRIVATE dafka_msg_key_t *
    dafka_msg_key_dup (dafka_msg_key_t *self);

DAFKA_PRIVATE uint32_t
    dafka_msg_key_hash (dafka_msg_key_t *self);

DAFKA_PRIVATE const char *
    dafka_msg_key_encode (dafka_msg_key_t *self, size_t *size_p);

DAFKA_PRIVATE int
    dafka_msg_key_decode (dafka_msg_key_t *self, const byte* buffer, size_t size);

DAFKA_PRIVATE int
    dafka_msg_key_cmp (const dafka_msg_key_t *self, const dafka_msg_key_t *other);

DAFKA_PRIVATE int
    dafka_msg_key_iter (dafka_msg_key_t *self, leveldb_iterator_t *iter);

DAFKA_PRIVATE void
    dafka_msg_key_iter_seek (dafka_msg_key_t *self, leveldb_iterator_t *iter);

// Set hashx key functions, comparator, hasher, dup and destroy
DAFKA_PRIVATE void
    dafka_msg_key_hashx_set (zhashx_t *hashx);


//  @end

#ifdef __cplusplus
}
#endif

#endif
