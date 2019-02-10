/*  =========================================================================
    dafka_store_reader -

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFAK_STORE_READER_H_INCLUDED
#define DAFAK_STORE_READER_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    const char* writer_address;
    leveldb_t *db;
    zconfig_t *config;
} dafka_store_reader_args_t;

typedef struct _dafka_store_reader_t dafka_store_reader_t;

//  Create new dafka_store_reader actor instance.
DAFKA_EXPORT void
    dafka_store_reader_actor (zsock_t *pipe, dafka_store_reader_args_t *args);

#ifdef __cplusplus
}
#endif

#endif
