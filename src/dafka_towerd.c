/*  =========================================================================
    dafka_towerd - description

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
    dafka_towerd -
@discuss
@end
*/

#include "dafka_classes.h"

int main (int argc, char** argv) {
    zargs_t *args = zargs_new (argc, argv);

    if (zargs_hasx (args, "--help", "-h", NULL)) {
        puts ("Usage: dafka_towerd [-c config] [--pub tower-pub-address] [--sub tower-sub-address] [--verbose]");
        return 0;
    }

    zconfig_t *config;

    if (zargs_has (args, "-c"))
        config = zconfig_load (zargs_get (args, "-c"));
    else
        config = zconfig_new ("root", NULL);

    if (zargs_has (args, "--verbose"))
        zconfig_put (config, "tower/verbose", "1");

    zactor_t *tower = zactor_new (dafka_tower_actor, config);

    char* command = zstr_recv (tower);
    zstr_free (&command); // Interrupted

    zconfig_destroy (&config);
    zargs_destroy (&args);
    zactor_destroy (&tower);

    return 0;
}
