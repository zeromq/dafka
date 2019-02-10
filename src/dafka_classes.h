/*  =========================================================================
    dafka_classes - private header file

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
#  THIS FILE IS 100% GENERATED BY ZPROJECT; DO NOT EDIT EXCEPT EXPERIMENTALLY  #
#  Read the zproject/README.md for information about making permanent changes. #
################################################################################
    =========================================================================
*/

#ifndef DAFKA_CLASSES_H_INCLUDED
#define DAFKA_CLASSES_H_INCLUDED

//  Platform definitions, must come first
#include "platform.h"

//  External API
#include "../include/dafka.h"

//  Extra headers

//  Opaque class structures to allow forward references
#ifndef DAFKA_MSG_KEY_T_DEFINED
typedef struct _dafka_msg_key_t dafka_msg_key_t;
#define DAFKA_MSG_KEY_T_DEFINED
#endif
#ifndef DAFKA_HEAD_KEY_T_DEFINED
typedef struct _dafka_head_key_t dafka_head_key_t;
#define DAFKA_HEAD_KEY_T_DEFINED
#endif
#ifndef DAFKA_UTIL_T_DEFINED
typedef struct _dafka_util_t dafka_util_t;
#define DAFKA_UTIL_T_DEFINED
#endif

//  Internal API

#include "dafka_msg_key.h"
#include "dafka_head_key.h"
#include "dafka_util.h"

//  *** To avoid double-definitions, only define if building without draft ***
#ifndef DAFKA_BUILD_DRAFT_API

//  *** Draft method, defined for internal use only ***
//  Self test of this class.
DAFKA_PRIVATE void
    dafka_util_test (bool verbose);

//  Self test for private classes
DAFKA_PRIVATE void
    dafka_private_selftest (bool verbose, const char *subtest);

#endif // DAFKA_BUILD_DRAFT_API

#endif
