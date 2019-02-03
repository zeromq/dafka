/*  =========================================================================
    dafka - ZeroMQ messaging middleware

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_H_H_INCLUDED
#define DAFKA_H_H_INCLUDED

//  Include the project library file
#include "dafka_library.h"

//  Add your own public definitions here, if you need them

typedef struct {
    const char *topic;
    zconfig_t *config;
} dafka_producer_args_t;

#endif
