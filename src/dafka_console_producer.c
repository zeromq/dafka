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

    dafka_producer_args_t producer_args =  { topic, config};
    zactor_t *producer = zactor_new (dafka_producer, &producer_args);

    char *content = NULL;
    size_t size = 0;

    dafka_producer_msg_t *msg = dafka_producer_msg_new ();

    while (true) {
        int64_t content_size = getline (&content, &size, stdin);
        if (content_size == -1)
            break;

        while (content[content_size - 1] == '\n' || content[content_size - 1] == '\r' || content[content_size - 1] == EOF)
            content_size--;

        dafka_producer_msg_set_content_buffer (msg, (const byte *) content, (size_t) content_size);
        dafka_producer_msg_send (msg, producer);
    }

    zstr_free (&content);
    dafka_producer_msg_destroy (&msg);
    zactor_destroy (&producer);
    zargs_destroy (&args);
    zconfig_destroy (&config);

    return 0;
}
