/*  =========================================================================
    dafka_tower -

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

/*
@header
    dafka_tower -
@discuss
@end
*/

#include "dafka_classes.h"

//  Structure of our actor

struct _dafka_tower_t {
    zsock_t *pipe;              //  Actor command pipe
    zpoller_t *poller;          //  Socket poller
    bool terminated;            //  Did caller ask us to quit?
    bool verbose;               //  Verbose logging enabled?
    zsock_t *xsub;
    zsock_t *xpub;
};


//  --------------------------------------------------------------------------
//  Create a new dafka_tower instance

static dafka_tower_t *
dafka_tower_new (zsock_t *pipe, zconfig_t *config)
{
    dafka_tower_t *self = (dafka_tower_t *) zmalloc (sizeof (dafka_tower_t));
    assert (self);

    self->pipe = pipe;
    self->terminated = false;

    if (atoi(zconfig_get (config, "tower/verbose", "0")))
        self->verbose = true;

    const char* sub_address = zconfig_get (config, "tower/sub_address", "tcp://*:5556");
    const char* pub_address = zconfig_get (config, "tower/pub_address", "tcp://*:5557");

    self->xpub = zsock_new_xpub (NULL);
    self->xsub = zsock_new_xsub (NULL);

    zsock_set_xpub_welcome_msg (self->xpub, "W");

    zsock_bind (self->xpub, "%s", pub_address);
    zsock_bind (self->xsub, "%s", sub_address);

    zsys_info ("Tower: xsub listening on %s", sub_address);
    zsys_info ("Tower: xpub listening on %s", pub_address);

    self->poller = zpoller_new (self->pipe, self->xsub, self->xpub, NULL);
    zpoller_set_nonstop (self->poller, true);

    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_tower instance

static void
dafka_tower_destroy (dafka_tower_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_tower_t *self = *self_p;

        zsock_destroy (&self->xpub);
        zsock_destroy (&self->xsub);

        //  Free object itself
        zpoller_destroy (&self->poller);
        free (self);
        *self_p = NULL;
    }
}

//  Here we handle incoming message from the node

static void
dafka_tower_recv_api (dafka_tower_t *self)
{
    //  Get the whole message of the pipe in one go
    zmsg_t *request = zmsg_recv (self->pipe);
    if (!request)
       return;        //  Interrupted

    char *command = zmsg_popstr (request);
    if (streq (command, "VERBOSE"))
        self->verbose = true;
    else
    if (streq (command, "$TERM")) {
        //  The $TERM command is send by zactor_destroy() method
        self->terminated = true;
    }
    else {
        zsys_error ("invalid command '%s'", command);
        assert (false);
    }
    zstr_free (&command);
    zmsg_destroy (&request);
}


//  --------------------------------------------------------------------------
//  This is the actor which runs in its own thread.

void
dafka_tower_actor (zsock_t *pipe, void *args)
{
    dafka_tower_t * self = dafka_tower_new (pipe, args);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (self->pipe, 0);

    zsys_info ("Tower: tower is running...");

    while (!self->terminated) {
        zsock_t *which = (zsock_t *) zpoller_wait (self->poller, -1);
        if (which == self->pipe)
            dafka_tower_recv_api (self);
        else if (which == self->xsub) {
            zmsg_t* msg = zmsg_recv (self->xsub);
            zmsg_send (&msg, self->xpub);
        } else if (which == self->xpub) {
            zframe_t * subscription = zframe_recv (self->xpub);

            // We drop welcome subscription, no need to forward them
            if (zframe_size (subscription) >= 2 && zframe_data (subscription)[1] == 'W') {
                zframe_destroy (&subscription);
            } else {
                zframe_send (&subscription, self->xsub, 0);
            }
        }
    }

    dafka_tower_destroy (&self);

    zsys_info ("Tower: tower stopped");
}

//  --------------------------------------------------------------------------
//  Self test of this actor.

// If your selftest reads SCMed fixture data, please keep it in
// src/selftest-ro; if your test creates filesystem objects, please
// do so under src/selftest-rw.
// The following pattern is suggested for C selftest code:
//    char *filename = NULL;
//    filename = zsys_sprintf ("%s/%s", SELFTEST_DIR_RO, "mytemplate.file");
//    assert (filename);
//    ... use the "filename" for I/O ...
//    zstr_free (&filename);
// This way the same "filename" variable can be reused for many subtests.
#define SELFTEST_DIR_RO "src/selftest-ro"
#define SELFTEST_DIR_RW "src/selftest-rw"

void
dafka_tower_test (bool verbose)
{
    printf (" * dafka_tower: ");
    //  @selftest
    //  Simple create/destroy test
    zactor_t *dafka_tower = zactor_new (dafka_tower_actor, NULL);
    assert (dafka_tower);

    zactor_destroy (&dafka_tower);
    //  @end

    printf ("OK\n");
}
