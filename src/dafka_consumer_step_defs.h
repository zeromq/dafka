/*  =========================================================================
    dafka_consumer_step_defs - class description

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of DAFKA the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_CONSUMER_STEP_DEFS_H_INCLUDED
#define DAFKA_CONSUMER_STEP_DEFS_H_INCLUDED

#if defined (HAVE_CUCUMBER)
#include <cucumber_c.h>

typedef struct _dafka_consumer_state dafka_consumer_state_t;

DAFKA_EXPORT dafka_consumer_state_t *
    dafka_consumer_state_new (bool verbose);

DAFKA_EXPORT void
    dafka_consumer_state_destroy (dafka_consumer_state_t **self_p);

DAFKA_EXPORT void
    register_dafka_consumer_step_defs (cucumber_t *cucumber);

#endif

#endif
