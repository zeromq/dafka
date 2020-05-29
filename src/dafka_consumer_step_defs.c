/*  =========================================================================
    dafka_consumer_step_defs - description

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

typedef struct _consumer_protocol_state {
    zconfig_t *config;
    zactor_t *tower;
    zactor_t *test_peer;
    zactor_t *consumer;
} consumer_protocol_state_t;

consumer_protocol_state_t *
consumer_protocol_state_new (bool verbose)
{
    consumer_protocol_state_t *self = (consumer_protocol_state_t *) zmalloc (sizeof (consumer_protocol_state_t));
    if (zsys_file_exists (SELFTEST_DIR_RW "/storedb")) {
        zdir_t *store_dir = zdir_new (SELFTEST_DIR_RW "/storedb", NULL);
        zdir_remove (store_dir, true);
        zdir_destroy (&store_dir);
    }

    self->config = zconfig_new ("root", NULL);
    zconfig_put (self->config, "test/verbose", verbose ? "1" : "0");
    zconfig_put (self->config, "beacon/interval", "50");
    zconfig_put (self->config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (self->config, "beacon/sub_address", "inproc://consumer-tower-sub");
    zconfig_put (self->config, "beacon/pub_address", "inproc://consumer-tower-pub");
    zconfig_put (self->config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (self->config, "tower/sub_address", "inproc://consumer-tower-sub");
    zconfig_put (self->config, "tower/pub_address", "inproc://consumer-tower-pub");
    zconfig_put (self->config, "consumer/verbose", verbose ? "1" : "0");
    zconfig_put (self->config, "producer/verbose", verbose ? "1" : "0");
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
consumer_protocol_state_destroy (consumer_protocol_state_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        consumer_protocol_state_t *self = *self_p;
        //  Free class properties
        zactor_destroy (&self->tower);
        zactor_destroy (&self->test_peer);
        zactor_destroy (&self->consumer);
        zconfig_destroy (&self->config);
        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}

void
given_a_dafka_consumer_with_offset_reset (cucumber_step_def_t *self, void *state_p)
{
    consumer_protocol_state_t *state = (consumer_protocol_state_t *) state_p;
    const char *offset_reset;
    FETCH_PARAMS (&offset_reset);

    zconfig_put (state->config, "consumer/offset/reset", offset_reset);

    state->consumer = zactor_new (dafka_consumer, state->config);
    assert (state->consumer);
    zclock_sleep (250); // Make sure consumer is connected to test_peer before continuing
}

void
given_no_subscription (cucumber_step_def_t *self, void *state_p)
{
    // Nothing to do
}

void
given_a_subscription (cucumber_step_def_t *self, void *state_p)
{
    consumer_protocol_state_t *state = (consumer_protocol_state_t *) state_p;
    const char *topic;
    FETCH_PARAMS (&topic);

    //  WHEN consumer subscribes to topic 'hello'
    int rc = dafka_consumer_subscribe (state->consumer, topic);
    assert_that_int (rc, int_equals, 0);

    if (streq (zconfig_get (state->config, "consumer/offset/reset", ""), "earliest")) {
        //  THEN the consumer will send a GET_HEADS msg for the topic 'hello'
        dafka_proto_t *msg = dafka_test_peer_recv (state->test_peer);

        assert_that_char (dafka_proto_id (msg), char_equals, DAFKA_PROTO_GET_HEADS);
        assert_that_str (dafka_proto_topic (msg), streq, topic);

        dafka_proto_destroy (&msg);
    }
    zclock_sleep (250); // Make sure subscription is active
}

void
when_the_consumer_subscribes(cucumber_step_def_t *self, void *state_p)
{
    consumer_protocol_state_t *state = (consumer_protocol_state_t *) state_p;
    const char *topic;
    FETCH_PARAMS (&topic);

    int rc = dafka_consumer_subscribe (state->consumer, topic);
    assert_that_int (rc, int_equals, 0);
    zclock_sleep (250); // Make sure subscription is active
}

void
when_a_store_hello_is_sent (cucumber_step_def_t *self, void *state_p)
{
    consumer_protocol_state_t *state = (consumer_protocol_state_t *) state_p;

    char *consumer_address = dafka_consumer_address (state->consumer);
    dafka_test_peer_send_store_hello (state->test_peer, consumer_address);
}

void when_a_record_is_sent (cucumber_step_def_t *self, void *state_p)
{
    consumer_protocol_state_t *state = (consumer_protocol_state_t *) state_p;
    const char *sequence, *content, *topic;
    FETCH_PARAMS (&sequence, &content, &topic);

    dafka_test_peer_send_record (state->test_peer, topic, atoi (sequence), content);
}

void
then_the_consumer_will_send_get_heads (cucumber_step_def_t *self, void *state_p)
{
    consumer_protocol_state_t *state = (consumer_protocol_state_t *) state_p;
    const char *topic;
    FETCH_PARAMS (&topic);

    dafka_proto_t *msg = dafka_test_peer_recv (state->test_peer);

    assert_that_char (dafka_proto_id (msg), char_equals, DAFKA_PROTO_GET_HEADS);
    assert_that_str (dafka_proto_topic (msg), streq, topic);

    dafka_proto_destroy (&msg);
}

void
then_the_consumer_responds_with_consumer_hello_containing_n_topics (cucumber_step_def_t *self, void *state_p)
{
    consumer_protocol_state_t *state = (consumer_protocol_state_t *) state_p;
    const char *topic_count;
    FETCH_PARAMS (&topic_count);

    dafka_proto_t *msg = dafka_test_peer_recv (state->test_peer);
    int no_of_subjects = atoi (topic_count);

    assert_that_char (dafka_proto_id (msg), char_equals, 'W');
    assert_that_size_t (zlist_size (dafka_proto_subjects (msg)), size_t_equals, no_of_subjects);

    dafka_proto_destroy (&msg);
}

void
then_the_consumer_will_send_a_fetch_message (cucumber_step_def_t *self, void *state_p)
{
    consumer_protocol_state_t *state = (consumer_protocol_state_t *) state_p;
    const char *topic, *sequence;
    FETCH_PARAMS (&topic, &sequence);

    dafka_proto_t *msg = dafka_test_peer_recv (state->test_peer);

    assert_that_char (dafka_proto_id (msg), char_equals, DAFKA_PROTO_FETCH);
    assert_that_str (dafka_proto_subject (msg), streq, topic);
    assert_that_uint64_t (dafka_proto_sequence (msg), uint64_t_equals, (uint64_t) atoi (sequence));

    dafka_proto_destroy (&msg);
}

void
then_a_consumer_msg_is_sent (cucumber_step_def_t *self, void *state_p)
{
    consumer_protocol_state_t *state = (consumer_protocol_state_t *) state_p;
    const char *topic, *content;
    FETCH_PARAMS (&topic, &content);

    dafka_consumer_msg_t *c_msg = dafka_consumer_msg_new ();
    dafka_consumer_msg_recv (c_msg, state->consumer);

    assert_that_str (dafka_consumer_msg_subject (c_msg), streq, topic);

    const byte *msg_content = dafka_consumer_msg_content (c_msg);
    assert_that_str ((const char *) msg_content, streq, content);

    dafka_consumer_msg_destroy (&c_msg);
}

STEP_DEFS(dafka_consumer, consumer_protocol_state_new, consumer_protocol_state_destroy) {
    GIVEN("a dafka consumer with offset reset (\\w+)",
          given_a_dafka_consumer_with_offset_reset)

    GIVEN("no subscriptions",
          given_no_subscription)

    GIVEN("a subscription to topic (\\w+)",
          given_a_subscription)

    WHEN("the consumer subscribes to topic (\\w+)",
         when_the_consumer_subscribes)

    WHEN("a STORE-HELLO command is send by a store",
         when_a_store_hello_is_sent)

    WHEN("a RECORD message with sequence (\\d+) and content '([^']+)' is send on topic (\\w+)",
         when_a_record_is_sent)

    THEN("the consumer will send a GET_HEADS message for topic (\\w+)",
         then_the_consumer_will_send_get_heads)

    THEN("the consumer responds with CONSUMER-HELLO containing (\\d+) topics?",
        then_the_consumer_responds_with_consumer_hello_containing_n_topics)

    THEN("the consumer will send a FETCH message for topic (\\w+) with sequence (\\d+)",
        then_the_consumer_will_send_a_fetch_message);

    THEN("a consumer_msg is send to the user with topic (\\w+) and content '([^']+)'",
        then_a_consumer_msg_is_sent);
}
#else
int
main()
{
    printf ("The cucumber_c library to run the step definitions.\n");
    printf ("Please install cucumber_c then reconfigure and re-compile this project!\n");
}
#endif
