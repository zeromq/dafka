/*  =========================================================================
    dafka_producer_step_defs - class description

    Copyright (c) the Contributors as noted in the AUTHORS file. This
    file is part of DAFKA, a decentralized distributed streaming
    platform: http://zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_PRODUCER_STEP_DEFS_H_INCLUDED
#define DAFKA_PRODUCER_STEP_DEFS_H_INCLUDED

#if defined (HAVE_CUCUMBER)
#include <cucumber_c.h>

typedef struct _dafka_producer_state dafka_producer_state_t;

DAFKA_EXPORT dafka_producer_state_t *
    dafka_producer_state_new (bool verbose);

DAFKA_EXPORT void
    dafka_producer_state_destroy (dafka_producer_state_t **self_p);

DAFKA_EXPORT void
    register_dafka_producer_step_defs (cucumber_t *cucumber);
#endif

#endif
