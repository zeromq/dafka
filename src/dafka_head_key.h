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

#ifndef DAFKA_HEAD_KEY_H_INCLUDED
#define DAFKA_HEAD_KEY_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

//  @interface
//  Create a new dafka_head_key
DAFKA_PRIVATE dafka_head_key_t *
    dafka_head_key_new (void);

//  Destroy the dafka_head_key
DAFKA_PRIVATE void
    dafka_head_key_destroy (dafka_head_key_t **self_p);

DAFKA_PRIVATE void
    dafka_head_key_set (dafka_head_key_t *self, const char* subject, const char* address);

DAFKA_PRIVATE const char *
    dafka_head_key_subject (dafka_head_key_t *self);

DAFKA_PRIVATE const char *
    dafka_head_key_address (dafka_head_key_t *self);

DAFKA_PRIVATE dafka_head_key_t *
    dafka_head_key_dup (dafka_head_key_t *self);

DAFKA_PRIVATE uint32_t
    dafka_head_key_hash (dafka_head_key_t *self);

// Encode head key as bytes for leveldb
DAFKA_PRIVATE const char *
    dafka_head_key_encode (dafka_head_key_t *self, size_t *size_p);

DAFKA_PRIVATE int
    dafka_head_key_decode (dafka_head_key_t *self, byte* buffer, size_t size);

DAFKA_PRIVATE int
    dafka_head_key_cmp (const dafka_head_key_t *self, const dafka_head_key_t *other);

// Set hashx key functions, comparator, hasher, dup and destroy
DAFKA_PRIVATE void
    dafka_head_key_hashx_set (zhashx_t *hashx);

//  @end

#ifdef __cplusplus
}
#endif

#endif
