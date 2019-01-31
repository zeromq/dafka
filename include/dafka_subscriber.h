/*  =========================================================================
    dafka_subscriber - class description

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_SUBSCRIBER_H_INCLUDED
#define DAFKA_SUBSCRIBER_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

//  @interface
//  Create a new dafka_subscriber
DAFKA_EXPORT dafka_subscriber_t *
    dafka_subscriber_new (void);

//  Destroy the dafka_subscriber
DAFKA_EXPORT void
    dafka_subscriber_destroy (dafka_subscriber_t **self_p);

//  Self test of this class
DAFKA_EXPORT void
    dafka_subscriber_test (bool verbose);

//  @end

#ifdef __cplusplus
}
#endif

#endif
