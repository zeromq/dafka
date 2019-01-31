/*  =========================================================================
    dafka_stored - description

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
    dafka_stored -
@discuss
@end
*/

#include "dafka_classes.h"


int main (int argc, char** argv) {
    zargs_t *args = zargs_new (argc, argv);

    if (zargs_hasx (args, "--help", "-h", NULL)) {
        puts ("Usage: dafka_store endpoint publisher_endpoints");
        return 0;
    }

    const char *endpoint = zargs_first (args);
    const char *publisher_endpoints = zargs_next (args);

    zargs_destroy (&args);

    const char *store_args[2] = {endpoint, publisher_endpoints};
    zactor_t *store = zactor_new (dafka_store_actor, store_args);

    char* command = zstr_recv (store);
    zstr_free (&command); // Interrupted

    zactor_destroy (&store);

    return 0;
}
