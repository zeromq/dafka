/*  =========================================================================
    dafka_consumer - class description

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_CONSUMER_H_INCLUDED
#define DAFKA_CONSUMER_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

//  @warning THE FOLLOWING @INTERFACE BLOCK IS AUTO-GENERATED BY ZPROJECT
//  @warning Please edit the model at "api/dafka_consumer.api" to make changes.
//  @interface
//  This is a stable class, and may not change except for emergencies. It
//  is provided in stable builds.
//
DAFKA_EXPORT void
    dafka_consumer (zsock_t *pipe, void *args);

//
DAFKA_EXPORT int
    dafka_consumer_subscribe (zactor_t *self, const char *subject);

//  Self test of this class.
DAFKA_EXPORT void
    dafka_consumer_test (bool verbose);

//  @end

#ifdef __cplusplus
}
#endif

#endif
