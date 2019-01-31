/*  =========================================================================
    dafka_subscriber -

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
//  Create new dafka_subscriber actor instance.
//  @TODO: Describe the purpose of this actor!
//
//      zactor_t *dafka_subscriber = zactor_new (dafka_subscriber, "publisher-address");
//
//  Destroy dafka_subscriber instance.
//
//      zactor_destroy (&dafka_subscriber);
//
//  Enable verbose logging of commands and activity:
//
//      zstr_send (dafka_subscriber, "VERBOSE");
//
//  This is the dafka_subscriber constructor as a zactor_fn;
DAFKA_EXPORT void
    dafka_subscriber_actor (zsock_t *pipe, void *args);

//  Self test of this actor
DAFKA_EXPORT void
    dafka_subscriber_test (bool verbose);
//  @end

#ifdef __cplusplus
}
#endif

#endif
