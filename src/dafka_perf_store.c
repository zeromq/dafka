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

int main (int argc, char *argv [])
{
    zsys_set_pipehwm (1000000);

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

    // Give everyone time to connect
    zclock_sleep (1000);

    // Sending all the messages now
    dafka_producer_msg_t *msg = dafka_producer_msg_new ();

    void *watch = zmq_stopwatch_start ();
    for (int i = 0; i < count; ++i) {
        dafka_producer_msg_init_content (msg, (size_t) size);
        dafka_producer_msg_send (msg, producer);
    }

    printf ("Done publishing\n");

    // Now waiting for the subscriber confirmation
    zactor_destroy (&producer);

    unsigned long elapsed = zmq_stopwatch_stop (watch);
    if (elapsed == 0)
        elapsed = 1;

    double throughput = ((double) count / (double) elapsed * 1000000);
    double megabits = ((double) throughput * size * 8) / 1000000;

    printf ("message size: %d [B]\n", size);
    printf ("message count: %d\n", count);
    printf ("mean throughput: %d [msg/s]\n", (int) throughput);
    printf ("mean throughput: %.3f [Mb/s]\n", megabits);

    dafka_producer_msg_destroy (&msg);
    zargs_destroy (&args);
    zconfig_destroy (&config);
}
