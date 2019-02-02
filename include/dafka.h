/*  =========================================================================
    dafka - ZeroMQ messaging middleware

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_H_H_INCLUDED
#define DAFKA_H_H_INCLUDED

//  Include the project library file
#include "dafka_library.h"

//  Add your own public definitions here, if you need them

//  Static helper methods

static void
uint64_destroy (void **item_p)
{
    assert (item_p);
    if (*item_p) {
        uint64_t *item = (uint64_t *) *item_p;
        free (item);
        *item_p = NULL;
    }
}

static void *
uint64_dup (const void *item)
{
    uint64_t *value = (uint64_t *) malloc (sizeof (uint64_t));
    memcpy (value, item, sizeof (uint64_t));
    return value;
}

static int
uint64_cmp (const void *item1, const void *item2)
{
    uint64_t value1 = *((uint64_t *) item1);
    uint64_t value2 = *((uint64_t *) item2);
    if (value1 < value2)
        return -1;
    else
    if (value1 == value2)
        return 0;
    else
    if (value1 > value2)
        return 1;
}

static size_t
uint64_hash (const void *item)
{
    uint64_t value = *((uint64_t *) item);
    return (size_t) value;
}


#endif
