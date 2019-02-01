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

    if (zargs_hasx (args, "--help", "-h", NULL) || zargs_arguments (args) != 2) {
        puts ("Usage: dafka_console_producer endpoint topic");
        return 0;
    }

    const char *endpoint = zargs_first (args);
    const char *topic = zargs_next (args);

    zargs_destroy (&args);

    const char *publisher_args[] = { topic, endpoint };
    zactor_t *publisher = zactor_new (dafka_publisher_actor, publisher_args);

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

    return 0;
}
