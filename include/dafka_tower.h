/*  =========================================================================
    dafka_tower -

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_TOWER_H_INCLUDED
#define DAFKA_TOWER_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif


//  @interface
//  Create new dafka_tower actor instance.
//  @TODO: Describe the purpose of this actor!
//
//      zactor_t *dafka_tower = zactor_new (dafka_tower, NULL);
//
//  Destroy dafka_tower instance.
//
//      zactor_destroy (&dafka_tower);
//
//  Start dafka_tower actor.
//
//      zstr_sendx (dafka_tower, "START", NULL);
//
//  Stop dafka_tower actor.
//
//      zstr_sendx (dafka_tower, "STOP", NULL);
//
//  This is the dafka_tower constructor as a zactor_fn;
DAFKA_EXPORT void
    dafka_tower_actor (zsock_t *pipe, void *args);

//  Self test of this actor
DAFKA_EXPORT void
    dafka_tower_test (bool verbose);
//  @end

#ifdef __cplusplus
}
#endif

#endif
