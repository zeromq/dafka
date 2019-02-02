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

