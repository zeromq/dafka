/*  =========================================================================
    beacon -

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
    beacon -
@discuss
@end
*/

#include "dafka_classes.h"

//  Structure of our actor

struct _dafka_beacon_t {
    zsock_t *pipe;              //  Actor command pipe
    zpoller_t *poller;          //  Socket poller
    ztimerset_t *timerset;
    bool terminated;            //  Did caller ask us to quit?
    bool verbose;               //  Verbose logging enabled?

    int timer_id;
    bool connected;
    zhashx_t *peers;

    char *sender;
    int port;

    int beacon_timeout;
    int interval;
    char *tower_pub_address;
    char *tower_sub_address;

    zsock_t *pub;
    zsock_t *sub;
};

int zconfig_get_int (zconfig_t* config, const char *path, int default_value) {
    char default_text[256];
    snprintf (default_text, 255, "%d", default_value);

    char* value = zconfig_get (config, path, default_text);

    return atoi (value);
}

//  --------------------------------------------------------------------------
//  Create a new beacon instance

static dafka_beacon_t *
dafka_beacon_new (zsock_t *pipe, zconfig_t *config) {
    dafka_beacon_t *self = (dafka_beacon_t *) zmalloc (sizeof (dafka_beacon_t));
    assert (self);

    self->pipe = pipe;
    self->terminated = false;
    self->timerset = ztimerset_new ();
    self->peers = zhashx_new ();
    self->timer_id = -1;
    self->port = -1;

    if (atoi (zconfig_get (config, "beacon/verbose", "0")))
        self->verbose = true;

    self->beacon_timeout = zconfig_get_int(config, "beacon/timeout", 4000);
    self->interval = zconfig_get_int(config, "beacon/interval", 1000);
    self->tower_sub_address = strdup (zconfig_get (config, "beacon/sub_address","tcp://127.0.0.1:5556"));
    self->tower_pub_address = strdup (zconfig_get (config, "beacon/pub_address","tcp://127.0.0.1:5557"));

    // Creating publisher socket
    self->pub = zsock_new_pub (NULL);

    // Create Subscriber socket and subscribe to welcome message and beacon topic
    self->sub = zsock_new_sub (NULL, "W");
    zsock_set_subscribe (self->sub, "B");

    self->poller = zpoller_new (self->pipe, self->sub, NULL);
    zpoller_set_nonstop (self->poller, true);

    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the beacon instance

static void
beacon_destroy (dafka_beacon_t **self_p) {
    assert (self_p);
    if (*self_p) {
        dafka_beacon_t *self = *self_p;
        zpoller_destroy (&self->poller);
        ztimerset_destroy (&self->timerset);
        zhashx_destroy (&self->peers);
        zstr_free (&self->tower_pub_address);
        zstr_free (&self->tower_sub_address);
        zstr_free (&self->sender);
        zsock_destroy (&self->sub);
        zsock_destroy (&self->pub);

        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}

void
dafka_beacon_interval (int timer_id, dafka_beacon_t *self) {
    (void)timer_id;

    zsock_send (self->pub, "ssi", "B", self->sender, self->port);
}

//  Start this actor. Return a value greater or equal to zero if initialization
//  was successful. Otherwise -1.

static int
dafka_beacon_start (dafka_beacon_t *self) {
    assert (self);

    // Destroy old fields
    zstr_free (&self->sender);

    // Cancel old timer if running
    if (self->timer_id != -1)
        ztimerset_cancel (self->timerset, self->timer_id);

    int port;
    int rc = zsock_recv (self->pipe, "si", &self->sender, &port);

    if (rc == -1) {
        zsys_error ("Beacon: error while receiving start command");
        zsock_signal (self->pipe, 255);
        return -1;
    }

    self->port = port;

    // Enable the beacon timer
    self->timer_id = ztimerset_add (self->timerset, (size_t) self->interval, (ztimerset_fn *) dafka_beacon_interval, self);

    // Sending the first beacon immediately
    zsock_send (self->pub, "ssi", "B", self->sender, self->port);

    zsock_signal (self->pipe, 0);

    if (self->verbose)
        zsys_debug ("Beacon: started. port: %d interval: %d uuid: %s",
                    self->port, self->interval, self->sender);

    return 0;
}


//  Stop this actor. Return a value greater or equal to zero if stopping
//  was successful. Otherwise -1.

static int
dafka_beacon_stop (dafka_beacon_t *self) {
    assert (self);

    if (self->timer_id != -1)
        ztimerset_cancel (self->timerset, self->timer_id);

    self->timer_id = -1;
    self->port = -1;

    return 0;
}


//  Here we handle incoming message from the node

static void
dafka_beacon_recv_api (dafka_beacon_t *self) {
    //  Get the whole message of the pipe in one go

    char *command = zstr_recv (self->pipe);
    if (!command)
        return;        //  Interrupted

    if (streq (command, "START"))
        dafka_beacon_start (self);
    else
    if (streq (command, "STOP"))
        dafka_beacon_stop (self);
    else
    if (streq (command, "$TERM"))
        //  The $TERM command is send by zactor_destroy() method
        self->terminated = true;
    else {
        zsys_error ("invalid command '%s'", command);
        assert (false);
    }
    zstr_free (&command);
}


//  Here we handle incoming message from the tower

static void
dafka_beacon_recv_sub (dafka_beacon_t *self) {
    char *topic = zstr_recv (self->sub);

    if (streq (topic, "W")) {
        if (!self->connected) {
            self->connected = true;

            //  Signal actor successfully initiated
            zsock_signal (self->pipe, 0);

            if (self->verbose)
                zsys_info ("Beacon: connected to tower");
        }
    } else if (streq (topic, "B")) {
        char *sender;
        char *address;

        zsock_recv (self->sub, "ss", &sender, &address);

        // Drop our own beaconing
        if (strneq (self->sender, sender)) {
            int64_t* expire = zhashx_lookup (self->peers, address);

            if (expire == NULL) {
                expire = (int64_t *) zmalloc (sizeof (int64_t));
                *expire = zclock_time () + self->beacon_timeout;
                zhashx_insert (self->peers, address, expire);
                zhashx_freefn (self->peers, address, free);

                zsock_send (self->pipe, "ss", "CONNECT", address);

                // New node on the network, sending a beacon immediately
                if (self->port != -1)
                    zsock_send (self->pub, "ssi", "B", self->sender, self->port);
            } else {
                *expire = zclock_time () + self->beacon_timeout;
            }
        }

        zstr_free (&sender);
        zstr_free (&address);
    }

    zstr_free (&topic);
}


//  Here we remove dead peers

void
dafka_beacon_clear_dead_peers (int timer_id, dafka_beacon_t *self) {
    (void)timer_id;
    int64_t now = zclock_time ();

    for (int64_t * expire = zhashx_first (self->peers); expire != NULL; expire = zhashx_next (self->peers)) {
        if (now > *expire) {
            char *address = (char*) zhashx_cursor(self->peers);

            if (self->verbose)
                zsys_debug ("Beacon: peer dead %s", address);

            zsock_send (self->pipe, "ss", "DISCONNECT", address);
            zhashx_delete (self->peers, address);
        }
    }
}

//  --------------------------------------------------------------------------
//  This is the actor which runs in its own thread.

void
dafka_beacon_actor (zsock_t *pipe, void *args) {
    dafka_beacon_t *self = dafka_beacon_new (pipe, args);
    if (!self)
        return;          //  Interrupted

    //  Register the clear dead peers interval
    ztimerset_add (self->timerset, 1000, (ztimerset_fn *)dafka_beacon_clear_dead_peers, self);

    // Connect subscriber to tower
    int rc = zsock_attach (self->sub, self->tower_pub_address, false);
    if (rc == -1) {
        zsys_error ("Beacon: error while connecting subscriber to towers %s", self->tower_pub_address);
        zsock_signal (self->pipe, 255);

        beacon_destroy (&self);
        return;
    }

    // Connect publisher to tower
    rc = zsock_attach (self->pub, self->tower_sub_address, false);
    if (rc == -1) {
        zsys_error ("Beacon: error while connecting subscriber to towers %s", self->tower_pub_address);
        zsock_signal (self->pipe, 255);

        beacon_destroy (&self);

        return;
    }

    while (!self->terminated) {
        zsock_t *which = (zsock_t *) zpoller_wait (self->poller, ztimerset_timeout (self->timerset));
        ztimerset_execute (self->timerset);

        if (which == self->pipe)
            dafka_beacon_recv_api (self);
        else if (which == self->sub)
            dafka_beacon_recv_sub (self);
    }
    beacon_destroy (&self);
}

void
dafka_beacon_recv (zactor_t *self, zsock_t *sub, bool verbose, const char *log_prefix) {
    char *command = zstr_recv (self);
    char *address = zstr_recv (self);

    if (streq (command, "CONNECT")) {
        if (verbose)
            zsys_info ("%s: Connecting to %s", log_prefix, address);

        zsock_connect (sub, "%s", address);
    }
    else if (streq (command, "DISCONNECT"))
        zsock_disconnect (sub, "%s", address);
    else {
        zsys_error ("Transport: Unknown command %s", command);
        assert (false);
    }

    zstr_free (&address);
    zstr_free (&command);
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
dafka_beacon_test (bool verbose) {
    printf (" * dafka_beacon: ");
    //  @selftest
    //  Simple create/destroy test
//    zactor_t *beacon = zactor_new (dafka_beacon_actor, NULL);
//    assert (beacon);
//
//    zactor_destroy (&beacon);
    //  @end

    printf ("OK\n");
}
