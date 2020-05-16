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
consumer_protocol_state_new ()
{
    consumer_protocol_state_t *self = (consumer_protocol_state_t *) zmalloc (sizeof (consumer_protocol_state_t));
    if (zsys_file_exists (SELFTEST_DIR_RW "/storedb")) {
        zdir_t *store_dir = zdir_new (SELFTEST_DIR_RW "/storedb", NULL);
        zdir_remove (store_dir, true);
        zdir_destroy (&store_dir);
    }

    bool verbose = false;   //  TODO Add as parameter

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
t_subscribe_to_topic (zactor_t *consumer, const char* topic,
                      zactor_t *test_peer, zconfig_t *config)
{
    //  WHEN consumer subscribes to topic 'hello'
    int rc = dafka_consumer_subscribe (consumer, topic);
    assert (rc == 0);

    if (streq (zconfig_get (config, "consumer/offset/reset", ""), "earliest")) {
        //  THEN the consumer will send a GET_HEADS msg for the topic 'hello'
        dafka_proto_t *msg = dafka_test_peer_recv (test_peer);
        assert_get_heads_msg (msg, (char *) topic);
    }
}

void
given_a_dafka_consumer_with_no_subscription (cucumber_step_def_t *self, void *state_p)
{
    consumer_protocol_state_t *state = (consumer_protocol_state_t *) state_p;

    state->consumer = zactor_new (dafka_consumer, state->config);
    assert (state->consumer);
    zclock_sleep (250); // Make sure consumer is connected to test_peer before continuing
}

void
given_a_dafka_consumer_with_a_subscription (cucumber_step_def_t *self, void *state_p)
{
    consumer_protocol_state_t *state = (consumer_protocol_state_t *) state_p;
    const char *topic;
    FETCH_PARAMS (&topic);

    given_a_dafka_consumer_with_no_subscription (self, state_p);

    t_subscribe_to_topic (state->consumer, topic, state->test_peer, state->config);
    zclock_sleep (250);
}

void
when_a_store_hello_is_sent (cucumber_step_def_t *self, void *state_p)
{
    consumer_protocol_state_t *state = (consumer_protocol_state_t *) state_p;

    char *consumer_address = dafka_consumer_address (state->consumer);
    dafka_test_peer_send_store_hello (state->test_peer, consumer_address);
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


STEP_DEFS(protocol, consumer_protocol_state_new, consumer_protocol_state_destroy) {
    GIVEN("a dafka consumer with no subscriptions",
          given_a_dafka_consumer_with_no_subscription)

    GIVEN("a dafka consumer with a subscription to topic (\\w+)",
          given_a_dafka_consumer_with_a_subscription)

    WHEN("a STORE-HELLO command is sent by a store",
         when_a_store_hello_is_sent)

    THEN("the consumer responds with CONSUMER-HELLO containing (\\d+) topics?",
        then_the_consumer_responds_with_consumer_hello_containing_n_topics)
}
#else
int
main()
{
    printf ("The cucumber_c library to run the step definitions.\n");
    printf ("Please install cucumber_c then reconfigure and re-compile this project!\n");
}
#endif
