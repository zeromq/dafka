/*  =========================================================================
    dafka_perf_store - description

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

/*
@header
    dafka_perf_store -
@discuss
@end
*/

#include "dafka_classes.h"

typedef struct{
    zconfig_t *config;
    bool verbose;
    int count;
    const char *producer_address;
} subscriber_args;

void subscriber_actor (zsock_t *pipe, subscriber_args *args) {
    dafka_beacon_args_t beacon_args = {"Subscriber", args->config};
    zactor_t *beacon = zactor_new (dafka_beacon_actor, &beacon_args);
    zsock_t *subscriber = zsock_new_sub (NULL, NULL);
    dafka_proto_subscribe (subscriber, DAFKA_PROTO_ACK, args->producer_address);

    zpoller_t *poller = zpoller_new (beacon, subscriber, pipe, NULL);
    dafka_proto_t *msg = dafka_proto_new ();

    zsock_signal (pipe, 0);

    bool terminated = false;
    while (!terminated) {
        void *which = zpoller_wait (poller, -1);

        if (which == beacon)
            dafka_beacon_recv (beacon, subscriber, false, "");

        if (which == pipe) {
            char* command = zstr_recv (pipe);

            if (streq (command, "$TERM"))
                terminated = true;

            zstr_free (&command);
        }

        if (which == subscriber) {
            int rc = dafka_proto_recv (msg, subscriber);
            if (rc == -1)
                continue;

            assert (DAFKA_PROTO_ACK == dafka_proto_id (msg));

            if (args->verbose)
                zsys_info ("Subscriber: received ack %" PRIu64, dafka_proto_sequence (msg));

            if (args->count - 1 == dafka_proto_sequence (msg)) {
                zsock_signal (pipe, 0);

                // we can also exit the actor
                break;
            }
        }
    }

    dafka_proto_destroy (&msg);
    zpoller_destroy (&poller);
    zsock_destroy (&subscriber);
    zactor_destroy (&beacon);
}

int main (int argc, char *argv [])
{
    zargs_t *args = zargs_new (argc, argv);

    if (zargs_hasx (args, "--help", "-h", NULL) || zargs_arguments (args) != 2) {
        puts ("Usage: dafka_perf_store [--verbose] [-c config] [--pub tower-pub-address] [--sub tower-sub-address] count size");
        return 0;
    }

    zconfig_t *config;

    if (zargs_has (args, "-c"))
        config = zconfig_load (zargs_get (args, "-c"));
    else
        config = zconfig_new ("root", NULL);

    bool verbose = zargs_has (args, "--verbose");

    if (verbose) {
        zconfig_put (config, "beacon/verbose", "1");
        zconfig_put (config, "producer/verbose", "1");
    }

    if (zargs_has (args, "--pub"))
        zconfig_put (config, "beacon/pub_address", zargs_get (args, "--pub"));

    if (zargs_has (args, "--sub"))
        zconfig_put (config, "beacon/sub_address", zargs_get (args, "--sub"));

    int count = atoi (zargs_first (args));
    int size = atoi (zargs_next (args));

    // Creating the producer
    dafka_producer_args_t producer_args = {"$STORE_PERF", config};
    zactor_t *producer = zactor_new (dafka_producer, &producer_args);
    const char *producer_address = dafka_producer_address (producer);

    // Creating the subscriber which wait for ACK msg
    subscriber_args sub_args = {config, verbose, count,  producer_address};
    zactor_t *subscriber = zactor_new ((zactor_fn *) subscriber_actor, &sub_args);

    // Give everyone time to connect
    zclock_sleep (1000);

    // Sending all the messages now
    dafka_producer_msg_t *msg = dafka_producer_msg_new ();

    void *watch = zmq_stopwatch_start ();
    for (int i = 0; i < count; ++i) {
        dafka_producer_msg_init_content (msg, (size_t) size);
        dafka_producer_msg_send (msg, producer);
    }

    // Now waiting for the subscriber confirmation
    zsock_wait (subscriber);

    unsigned long elapsed = zmq_stopwatch_stop (watch);
    if (elapsed == 0)
        elapsed = 1;

    double throughput = ((double) count / (double) elapsed * 1000000);
    double megabits = ((double) throughput * size * 8) / 1000000;

    printf ("message size: %d [B]\n", size);
    printf ("message count: %d\n", count);
    printf ("mean throughput: %d [msg/s]\n", (int) throughput);
    printf ("mean throughput: %.3f [Mb/s]\n", megabits);

    zactor_destroy (&subscriber);
    dafka_producer_msg_destroy (&msg);
    zactor_destroy (&producer);
    zargs_destroy (&args);
    zconfig_destroy (&config);
}
