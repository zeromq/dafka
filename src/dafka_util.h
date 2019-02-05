/*  =========================================================================
    dafka_util - class description

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_UTIL_H_INCLUDED
#define DAFKA_UTIL_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

DAFKA_PRIVATE char *
generate_address ();

DAFKA_PRIVATE void
uint64_destroy (void **item_p);

DAFKA_PRIVATE void *
uint64_dup (const void *item);

DAFKA_PRIVATE int
uint64_cmp (const void *item1, const void *item2);

DAFKA_PRIVATE size_t
uint64_hash (const void *item);

DAFKA_PRIVATE size_t
uint64_put_le (byte* output, uint64_t value);

DAFKA_PRIVATE size_t
uint64_get_le (const byte* input, uint64_t *value);


//  @end

#ifdef __cplusplus
}
#endif

#endif
