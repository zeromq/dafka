/*  =========================================================================
    dafka_producer_step_defs - description

    Copyright (c) the Contributors as noted in the AUTHORS file. This
    file is part of DAFKA, a decentralized distributed streaming
    platform: http://zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

#include "dafka_classes.h"
#if defined (HAVE_CUCUMBER)
#include <cucumber_c.h>

#define SELFTEST_DIR_RW "src/selftest-rw"

typedef struct _dafka_producer_state {
    zconfig_t *config;
    zactor_t *tower;
    zactor_t *test_peer;
    zactor_t *producer;
    char *topic;
} dafka_producer_state_t;

dafka_producer_state_t *
dafka_producer_state_new (bool verbose)
{
    dafka_producer_state_t *self = (dafka_producer_state_t *) zmalloc (sizeof (dafka_producer_state_t));
    if (zsys_file_exists (SELFTEST_DIR_RW "/storedb")) {
        zdir_t *store_dir = zdir_new (SELFTEST_DIR_RW "/storedb", NULL);
        zdir_remove (store_dir, true);
        zdir_destroy (&store_dir);
    }

    self->config = zconfig_new ("root", NULL);
    zconfig_put (self->config, "test/verbose", verbose ? "1" : "0");
    zconfig_put (self->config, "beacon/interval", "50");
    zconfig_put (self->config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (self->config, "beacon/sub_address", "inproc://producer-tower-sub");
    zconfig_put (self->config, "beacon/pub_address", "inproc://producer-tower-pub");
    zconfig_put (self->config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (self->config, "tower/sub_address", "inproc://producer-tower-sub");
    zconfig_put (self->config, "tower/pub_address", "inproc://producer-tower-pub");
    zconfig_put (self->config, "producer/verbose", verbose ? "1" : "0");
    zconfig_put (self->config, "producer/terminate_unacked", "1");
    zconfig_put (self->config, "producer/head_interval", "100");
    zconfig_put (self->config, "consumer/verbose", verbose ? "1" : "0");
    zconfig_put (self->config, "store/verbose", verbose ? "1" : "0");
    zconfig_put (self->config, "store/db", SELFTEST_DIR_RW "/storedb");

    zconfig_put (self->config, "consumer/offset/reset", "earliest");

    self->tower = zactor_new (dafka_tower_actor, self->config);
    assert (self->tower);

    self->test_peer = zactor_new (dafka_test_peer, self->config);
    assert (self->test_peer);

    return self;
}

void
dafka_producer_state_destroy (dafka_producer_state_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_producer_state_t *self = *self_p;
        //  Free class properties
        zactor_destroy (&self->tower);
        zactor_destroy (&self->test_peer);
        zactor_destroy (&self->producer);
        zconfig_destroy (&self->config);
        zstr_free (&self->topic);
        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}

void
given_a_dafka_producer_for_topic (cucumber_step_def_t *self, void *state_p)
{
    dafka_producer_state_t *state = (dafka_producer_state_t *) state_p;
    const char *topic;
    FETCH_PARAMS (&topic);

    dafka_producer_args_t args = {topic, state->config};
    state->producer = zactor_new (dafka_producer, &args);
    assert (state->producer);
    zclock_sleep (250); // Make sure producer is connected to test_peer before continuing
    state->topic = strdup (topic);
}

void
given_a_dafka_producer_sent_a_record_message (cucumber_step_def_t *self, void *state_p)
{
    dafka_producer_state_t *state = (dafka_producer_state_t *) state_p;
    const char *content;
    FETCH_PARAMS (&content);

    dafka_producer_msg_t *msg = dafka_producer_msg_new ();
    dafka_producer_msg_set_content_str (msg, content);
    dafka_producer_msg_send (msg, state->producer);
    dafka_producer_msg_destroy (&msg);

    dafka_proto_t *received_msg = dafka_test_peer_recv (state->test_peer);
    assert_that_char (dafka_proto_id (received_msg), char_equals, DAFKA_PROTO_RECORD);
    dafka_proto_destroy (&received_msg);
}

void
when_a_dafka_producer_sends_a_record_message (cucumber_step_def_t *self, void *state_p)
{
    dafka_producer_state_t *state = (dafka_producer_state_t *) state_p;
    const char *content;
    FETCH_PARAMS (&content);

    dafka_producer_msg_t *msg = dafka_producer_msg_new ();
    dafka_producer_msg_set_content_str (msg, content);
    dafka_producer_msg_send (msg, state->producer);
    dafka_producer_msg_destroy (&msg);
}

void
when_the_producer_recieves_a_fetch_message (cucumber_step_def_t *self, void *state_p)
{
    dafka_producer_state_t *state = (dafka_producer_state_t *) state_p;
    const char *sequence;
    FETCH_PARAMS (&sequence);

    dafka_test_peer_send_fetch (state->test_peer,
                                dafka_producer_address (state->producer),
                                state->topic,
                                atoi (sequence),
                                1);
}

void
then_the_producer_will_send_a_direct_record_message (cucumber_step_def_t *self, void *state_p)
{
    dafka_producer_state_t *state = (dafka_producer_state_t *) state_p;
    const char *content;
    FETCH_PARAMS (&content);

    dafka_proto_t *msg = NULL;
    do {
        if (msg)
            dafka_proto_destroy (&msg);

        msg = dafka_test_peer_recv (state->test_peer);
    } while (dafka_proto_id (msg) == DAFKA_PROTO_HEAD);

    assert_that_char (dafka_proto_id (msg), char_equals, DAFKA_PROTO_DIRECT_RECORD);
    assert_that_str (dafka_proto_topic (msg), streq, dafka_test_peer_address (state->test_peer));
    assert_that_str (dafka_proto_subject (msg), streq, state->topic);
    zmq_msg_t *content_msg = dafka_proto_content (msg);
    char *data = (char *) zmq_msg_data (content_msg);
    assert_that_str (data, streq, content);

    dafka_proto_destroy (&msg);
}

void
then_a_producer_will_send_head_messages_at_regular_intervals (cucumber_step_def_t *self, void *state_p)
{
    dafka_producer_state_t *state = (dafka_producer_state_t *) state_p;

    for (int index = 0; index < 5; index++) {
        dafka_proto_t *msg = dafka_test_peer_recv (state->test_peer);

        if (dafka_proto_id (msg) != DAFKA_PROTO_RECORD) {
            assert_that_char (dafka_proto_id (msg), char_equals, DAFKA_PROTO_HEAD);
        }

        dafka_proto_destroy (&msg);
    }
}

STEP_DEFS(dafka_producer, dafka_producer_state_new, dafka_producer_state_destroy) {
    GIVEN("a dafka producer for topic (\\w+)", given_a_dafka_producer_for_topic)
    GIVEN("the producer sent a RECORD message with content '(\\w+)'",
          given_a_dafka_producer_sent_a_record_message)

    WHEN("the producer receives a FETCH message with sequence (\\d+)",
         when_the_producer_recieves_a_fetch_message)
    WHEN("the producer sends a RECORD message with content '(\\w+)'",
          when_a_dafka_producer_sends_a_record_message)

    THEN("the producer will send a DIRECT_RECORD message with content '(\\w+)'",
         then_the_producer_will_send_a_direct_record_message)
    THEN("the producer will send HEAD messages at regular intervals",
         then_a_producer_will_send_head_messages_at_regular_intervals)
}
#endif
