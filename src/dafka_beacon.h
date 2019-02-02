/*  =========================================================================
    dafka_beacon -

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_BEACON_H_INCLUDED
#define DAFKA_BEACON_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _dafka_beacon_t dafka_beacon_t;

//  @interface
//  Create new dafka_beacon actor instance.
//  @TODO: Describe the purpose of this actor!
//
//      zactor_t *dafka_beacon = zactor_new (dafka_beacon, NULL);
//
//  Destroy dafka_beacon instance.
//
//      zactor_destroy (&dafka_beacon);
//
//  Start dafka_beacon actor.
//
//      zstr_sendx (dafka_beacon, "START", NULL);
//
//  Stop dafka_beacon actor.
//
//      zstr_sendx (dafka_beacon, "STOP", NULL);
//
//  This is the dafka_beacon constructor as a zactor_fn;
DAFKA_EXPORT void
    dafka_beacon_actor (zsock_t *pipe, void *args);

DAFKA_PRIVATE void
    dafka_beacon_recv (zactor_t *self, zsock_t *sub, bool verbose, const char *log_prefix);

//  Self test of this actor
DAFKA_EXPORT void
    dafka_beacon_test (bool verbose);
//  @end

#ifdef __cplusplus
}
#endif

#endif
