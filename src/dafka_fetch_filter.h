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

#ifndef DAFKA_FETCH_FILTER_H_INCLUDED
#define DAFKA_FETCH_FILTER_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

//  @interface
//  Create a new dafka_fetch_filter
DAFKA_PRIVATE dafka_fetch_filter_t *
    dafka_fetch_filter_new (zsock_t *publisher, const char *address, bool verbose);

DAFKA_PRIVATE void dafka_fetch_filter_send (dafka_fetch_filter_t *self, const char *subject, const char *address, uint64_t sequence);

//  Destroy the dafka_fetch_filter
DAFKA_PRIVATE void
    dafka_fetch_filter_destroy (dafka_fetch_filter_t **self_p);


//  @end

#ifdef __cplusplus
}
#endif

#endif
