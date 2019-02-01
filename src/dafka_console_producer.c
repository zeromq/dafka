/*  =========================================================================
    dakfa_console_producer - description

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
    dakfa_console_producer -
@discuss
@end
*/

#include "dafka_classes.h"

int main (int argc, char *argv [])
{
    zsys_set_linger ((size_t ) -1);

    zargs_t *args = zargs_new (argc, argv);

    if (zargs_hasx (args, "--help", "-h", NULL) || zargs_arguments (args) != 1) {
        puts ("Usage: dafka_console_producer [--verbose] [-c config] [--pub tower-pub-address] [--sub tower-sub-address] topic");
        return 0;
    }

    zconfig_t *config;

    if (zargs_has (args, "-c"))
        config = zconfig_load (zargs_get (args, "-c"));
    else
        config = zconfig_new ("root", NULL);

    if (zargs_has (args, "--verbose")) {
        zconfig_put (config, "beacon/verbose", "1");
        zconfig_put (config, "producer/verbose", "1");
    }

    if (zargs_has (args, "--pub"))
        zconfig_put (config, "beacon/pub_address", zargs_get (args, "--pub"));

    if (zargs_has (args, "--sub"))
        zconfig_put (config, "beacon/sub_address", zargs_get (args, "--sub"));

    const char *topic = zargs_first (args);

    dafka_publisher_args_t publisher_args =  { topic, config};
    zactor_t *publisher = zactor_new (dafka_publisher_actor, &publisher_args);

    char *msg = NULL;
    size_t size = 0;

    while (true) {
        int64_t msg_size = getline (&msg, &size, stdin);
        if (msg_size == -1)
            break;

        while (msg[msg_size - 1] == '\n' || msg[msg_size - 1] == '\r' || msg[msg_size - 1] == EOF)
            msg_size--;

        zframe_t *frame = zframe_new (msg, (size_t) msg_size);

        dafka_publisher_publish (publisher, &frame);
    }

    zstr_free (&msg);
    zactor_destroy (&publisher);
    zargs_destroy (&args);
    zconfig_destroy (&config);

    return 0;
}
