/*  =========================================================================
    dafka_unacked_list - class description

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_UNACKED_LIST_H_INCLUDED
#define DAFKA_UNACKED_LIST_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

//  @interface
//  Create a new dafka_unacked_list
DAFKA_PRIVATE dafka_unacked_list_t *
    dafka_unacked_list_new (zsock_t *publisher, const char* address, const char *topic);

//  Destroy the dafka_unacked_list
DAFKA_PRIVATE void
    dafka_unacked_list_destroy (dafka_unacked_list_t **self_p);

DAFKA_PRIVATE uint64_t
dafka_unacked_list_push (dafka_unacked_list_t *self, zmq_msg_t *msg);

DAFKA_PRIVATE void
dafka_unacked_list_ack (dafka_unacked_list_t *self, uint64_t sequence);

DAFKA_PRIVATE void
dafka_unacked_list_send (dafka_unacked_list_t *self, const char* address, uint64_t sequence, uint32_t count);

DAFKA_PRIVATE bool
dafka_unacked_list_is_empty (dafka_unacked_list_t *self);

DAFKA_PRIVATE uint64_t
dafka_unacked_list_last_acked (dafka_unacked_list_t *self);


//  Self test of this class
DAFKA_PRIVATE void
    dafka_unacked_list_test (bool verbose);

//  @end

#ifdef __cplusplus
}
#endif

#endif
