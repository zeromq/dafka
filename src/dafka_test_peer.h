/*  =========================================================================
    dafka_test_peer - Peer to test interact with producers, consumers and
                      stores.

    Copyright (c) the Contributors as noted in the AUTHORS file. This
    file is part of DAFKA, a decentralized distributed streaming
    platform: http://zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_TEST_PEER_H_INCLUDED
#define DAFKA_TEST_PEER_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _dafka_test_peer_t dafka_test_peer_t;

//  @interface
//  Create new dafka_test_peer actor instance.
//
//      zactor_t *dafka_test_peer = zactor_new (dafka_test_peer, NULL);
//
//  Destroy dafka_test_peer instance.
//
//      zactor_destroy (&dafka_test_peer);
//
//  This is the dafka_test_peer constructor as a zactor_fn;
DAFKA_EXPORT void
    dafka_test_peer (zsock_t *pipe, void *args);

//  Self test of this actor
DAFKA_EXPORT void
    dafka_test_peer_test (bool verbose);

DAFKA_EXPORT void
    dafka_test_peer_send_store_hello (zactor_t *self, const char *consumer_address);

DAFKA_EXPORT void
    dafka_test_peer_send_head (zactor_t *self, const char *topic, uint64_t sequence);

DAFKA_EXPORT void
    dafka_test_peer_send_record (zactor_t *self, const char *topic, uint64_t sequence,  const char *content);

DAFKA_EXPORT dafka_proto_t *
    dafka_test_peer_recv (zactor_t *self);

DAFKA_EXPORT void
    assert_consumer_hello_msg (dafka_proto_t *msg, int no_of_subjects);

DAFKA_EXPORT void
    assert_get_heads_msg (dafka_proto_t *msg, const char *topic);

DAFKA_EXPORT void
    assert_fetch_msg (dafka_proto_t *msg, const char *topic, uint64_t sequence);

DAFKA_EXPORT void
    assert_consumer_msg (dafka_consumer_msg_t *msg, const char *topic, const char *content);
//  @end

#ifdef __cplusplus
}
#endif

#endif
