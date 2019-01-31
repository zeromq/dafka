/*  =========================================================================
    dafka_publisher - class description

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

//  @interface
//  Create a new dafka_publisher
DAFKA_EXPORT dafka_publisher_t *
    dafka_publisher_new (char *topic);

//  Destroy the dafka_publisher
DAFKA_EXPORT void
    dafka_publisher_destroy (dafka_publisher_t **self_p);

//  Publish content
DAFKA_EXPORT int
    dafka_publisher_publish (dafka_publisher_t *self, zframe_t *content);

//  Publish content reliable
DAFKA_EXPORT int
    dafka_publisher_publish_reliable (dafka_publisher_t *self, zframe_t *content);

//  Self test of this class
DAFKA_EXPORT void
    dafka_publisher_test (bool verbose);

//  @end

#ifdef __cplusplus
}
#endif

#endif
