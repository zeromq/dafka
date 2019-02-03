/*  =========================================================================
    dafka_publisher -

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_PUBLISHER_H_INCLUDED
#define DAFKA_PUBLISHER_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

#include "dafka.h"

typedef struct {
    const char *topic;
    zconfig_t *config;
} dafka_publisher_args_t;

//  @interface
//  Create new dafka_publisher actor instance.
//  @TODO: Describe the purpose of this actor!
//
//      zactor_t *dafka_publisher = zactor_new (dafka_publisher, NULL);
//
//  Destroy dafka_publisher instance.
//
//      zactor_destroy (&dafka_publisher);
//
//  Start dafka_publisher actor.
//
//      zstr_sendx (dafka_publisher, "START", NULL);
//
//  Stop dafka_publisher actor.
//
//      zstr_sendx (dafka_publisher, "STOP", NULL);
//
//  This is the dafka_publisher constructor as a zactor_fn;
DAFKA_EXPORT void
    dafka_publisher_actor (zsock_t *pipe, void *args);

//  Get the address the publisher
DAFKA_EXPORT const char *
    dafka_publisher_address (zactor_t *self);

//  Self test of this actor
DAFKA_EXPORT void
    dafka_publisher_test (bool verbose);
//  @end

#ifdef __cplusplus
}
#endif

#endif
