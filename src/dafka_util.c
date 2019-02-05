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

/*
@header
    dafka_util -
@discuss
@end
*/

#include "dafka_classes.h"

void
uint64_destroy (void **item_p)
{
    assert (item_p);
    if (*item_p) {
        uint64_t *item = (uint64_t *) *item_p;
        free (item);
        *item_p = NULL;
    }
}

void *
uint64_dup (const void *item)
{
    uint64_t *value = (uint64_t *) malloc (sizeof (uint64_t));
    memcpy (value, item, sizeof (uint64_t));
    return value;
}

int
uint64_cmp (const void *item1, const void *item2)
{
    uint64_t value1 = *((uint64_t *) item1);
    uint64_t value2 = *((uint64_t *) item2);
    if (value1 < value2)
        return -1;
    else
    if (value1 > value2)
        return 1;

    return 0;
}

size_t
uint64_hash (const void *item)
{
    uint64_t value = *((uint64_t *) item);
    return (size_t) value;
}

char *
generate_address () {
    zuuid_t *uuid = zuuid_new ();
    char *address = strdup (zuuid_str (uuid));
    zuuid_destroy (&uuid);
    return address;
}

size_t
uint64_put_be (byte* output, uint64_t value) {
    output[0] = (byte) (((value) >> 56) & 255);
    output[1] = (byte) (((value) >> 48) & 255);
    output[2] = (byte) (((value) >> 40) & 255);
    output[3] = (byte) (((value) >> 32) & 255);
    output[4] = (byte) (((value) >> 24) & 255);
    output[5] = (byte) (((value) >> 16) & 255);
    output[6] = (byte) (((value) >> 8) & 255);
    output[7] = (byte) (((value)) & 255);

    return 8;
}

size_t
uint64_get_be (const byte* input, uint64_t *value) {
    *value =  ((uint64_t) (input [0]) << 56)
           + ((uint64_t) (input [1]) << 48)
           + ((uint64_t) (input [2]) << 40)
           + ((uint64_t) (input [3]) << 32)
           + ((uint64_t) (input [4]) << 24)
           + ((uint64_t) (input [5]) << 16)
           + ((uint64_t) (input [6]) << 8)
           +  (uint64_t) (input [7]);

    return 8;
}