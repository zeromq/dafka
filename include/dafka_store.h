/*  =========================================================================
    dafka_store -

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_STORE_H_INCLUDED
#define DAFKA_STORE_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif


//  @interface
//  Create new dafka_store actor instance.
//  @TODO: Describe the purpose of this actor!
//
//      zactor_t *dafka_store = zactor_new (dafka_store, NULL);
//
//  Destroy dafka_store instance.
//
//      zactor_destroy (&dafka_store);
//
//  Enable verbose logging of commands and activity:
//
//      zstr_send (dafka_store, "VERBOSE");
//
//  Start dafka_store actor.
//
//      zstr_sendx (dafka_store, "START", NULL);
//
//  Stop dafka_store actor.
//
//      zstr_sendx (dafka_store, "STOP", NULL);
//
//  This is the dafka_store constructor as a zactor_fn;
DAFKA_EXPORT void
    dafka_store_actor (zsock_t *pipe, void *args);

//  Self test of this actor
DAFKA_EXPORT void
    dafka_store_test (bool verbose);
//  @end

#ifdef __cplusplus
}
#endif

#endif
