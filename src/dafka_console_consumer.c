/*  =========================================================================
    dafka_console_consumer - description

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
    dafka_console_consumer -
@discuss
@end
*/

#include "dafka_classes.h"

int main (int argc, char *argv [])
{
    zsys_set_linger ((size_t ) -1);

    zargs_t *args = zargs_new (argc, argv);

    if (zargs_hasx (args, "--help", "-h", NULL) || zargs_arguments (args) != 3) {
        puts ("Usage: dafka_console_consumer consumer_endpoint subscriber_endpoints topic");
        return 0;
    }

    const char *consumer_endpoint = zargs_first (args);
    const char *subscriber_endpoints = zargs_next (args);
    const char *topic = zargs_next (args);

    char *consumer_args[] = { (char *) subscriber_endpoints, (char *) consumer_endpoint };
    zactor_t *consumer = zactor_new (dafka_subscriber_actor, consumer_args);
    assert (consumer);

    int rc = zsock_send (consumer, "ss", "SUBSCRIBE", topic);
    assert (rc == 0);

    char *address;
    zframe_t *content;
    while (true) {
        zsock_brecv (consumer, "ssf", &topic, &address, &content);
        if (!content)
            break;      // Interrupted

        char *content_str = zframe_strdup (content);
        printf ("%s %s %s\n", topic, address, content_str);
        zstr_free (&content_str);
        zframe_destroy (&content);
    }

    zargs_destroy (&args);
    zactor_destroy (&consumer);

    return 0;
}
