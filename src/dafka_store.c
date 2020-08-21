/*  =========================================================================
    dafka_store -

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
    dafka_store -
@discuss
@end
*/

#include "dafka_classes.h"
#include <leveldb/c.h>

//  Structure of our actor

struct _dafka_store_t {
    bool verbose;               //  Verbose logging enabled?

    leveldb_t *db;
    leveldb_options_t *dboptions;

    char *address;

    zactor_t *writer;
    zactor_t *reader;
};


//  --------------------------------------------------------------------------
//  Create a new dafka_store instance

static dafka_store_t *
dafka_store_new (zconfig_t *config) {
    dafka_store_t *self = (dafka_store_t *) zmalloc (sizeof (dafka_store_t));
    assert (self);

    if (atoi (zconfig_get (config, "store/verbose", "0")))
        self->verbose = true;

    self->address = generate_address ();

    // Configure and open the leveldb database for the store
    const char *db_path = zconfig_get (config, "store/db", "storedb");
    char *err = NULL;
    self->dboptions = leveldb_options_create ();
    leveldb_options_set_create_if_missing (self->dboptions, 1);
    self->db = leveldb_open (self->dboptions, db_path, &err);
    if (err) {
        zsys_error ("Store: failed to open db %s", err);
        assert (false);
    }

    dafka_store_writer_args_t writer_args = {self->address, self->db, config};
    self->writer = zactor_new ((zactor_fn*) dafka_store_writer_actor, &writer_args);

    dafka_store_reader_args_t reader_args = {self->address, self->db, config};
    self->reader = zactor_new ((zactor_fn*) dafka_store_reader_actor, &reader_args);

    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_store instance

static void
dafka_store_destroy (dafka_store_t **self_p) {
    assert (self_p);
    if (*self_p) {
        dafka_store_t *self = *self_p;

        zactor_destroy (&self->reader);
        zactor_destroy (&self->writer);
        zstr_free (&self->address);
        leveldb_close (self->db);
        leveldb_options_destroy (self->dboptions);
        self->db = NULL;
        self->dboptions = NULL;

        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}

//  --------------------------------------------------------------------------
//  This is the actor which runs in its own thread.

void
dafka_store_actor (zsock_t *pipe, void *arg) {
    dafka_store_t *self = dafka_store_new ((zconfig_t *) arg);
    if (!self)
        return;          //  Interrupted

    //  Signal actor successfully initiated
    zsock_signal (pipe, 0);

    if (self->verbose)
        zsys_info ("Store: running...");

    bool terminated = false;
    while (!terminated) {
        char *command = zstr_recv (pipe);

        if (command == NULL)
            continue;

        if (streq (command, "$TERM"))
            terminated = true;

        zstr_free (&command);
    }

    bool verbose = self->verbose;

    dafka_store_destroy (&self);

    if (verbose)
        zsys_info ("Store: stopped");
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
dafka_store_test (bool verbose) {
    printf (" * dafka_store: ");
    //  @selftest
    // ----------------------------------------------------
    //  Cleanup old test artifacts
    // ----------------------------------------------------
    if (zsys_file_exists (SELFTEST_DIR_RW "/storedb")) {
        zdir_t *store_dir = zdir_new (SELFTEST_DIR_RW "/storedb", NULL);
        zdir_remove (store_dir, true);
        zdir_destroy (&store_dir);
    }

    //  Simple create/destroy test
    zconfig_t *config = zconfig_new ("root", NULL);
    zconfig_put (config, "beacon/verbose", verbose ? "1" : "0");
    zconfig_put (config, "beacon/sub_address", "inproc://store-tower-sub");
    zconfig_put (config, "beacon/pub_address", "inproc://store-tower-pub");
    zconfig_put (config, "tower/verbose", verbose ? "1" : "0");
    zconfig_put (config, "tower/sub_address", "inproc://store-tower-sub");
    zconfig_put (config, "tower/pub_address", "inproc://store-tower-pub");
    zconfig_put (config, "store/verbose", verbose ? "1" : "0");
    zconfig_put (config, "consumer/verbose", verbose ? "1" : "0");
    zconfig_put (config, "producer/verbose", verbose ? "1" : "0");
    zconfig_put (config, "store/db", SELFTEST_DIR_RW "/storedb");
    zconfig_put (config, "consumer/offset/reset", "earliest");

    // Creating the store
    zactor_t *tower = zactor_new (dafka_tower_actor, config);

    // Creating the publisher
    dafka_producer_args_t args = {"TEST", config};
    zactor_t *producer = zactor_new (dafka_producer, &args);

    // Producing before the store is alive, in order to test fetching between producer and store
    dafka_producer_msg_t *p_msg = dafka_producer_msg_new ();
    dafka_producer_msg_set_content_str (p_msg, "1");
    dafka_producer_msg_send (p_msg, producer);

    dafka_producer_msg_set_content_str (p_msg, "2");
    dafka_producer_msg_send (p_msg, producer);

    // Starting the store
    zactor_t *store = zactor_new (dafka_store_actor, config);
    zclock_sleep (100);

    // Producing another message
    dafka_producer_msg_set_content_str (p_msg, "3");
    dafka_producer_msg_send (p_msg, producer);
    zclock_sleep (100);

    // Killing the producer, to make sure the HEADs are coming from the store
    zactor_destroy (&producer);

    // Starting a consumer and check that consumer recv all 3 messages
    dafka_consumer_t *consumer = dafka_consumer_new (config);
    dafka_consumer_subscribe (consumer, "TEST");

    dafka_consumer_msg_t *c_msg = dafka_consumer_msg_new ();
    dafka_consumer_msg_recv (c_msg, consumer);
    assert (dafka_consumer_msg_streq (c_msg, "1"));

    dafka_consumer_msg_recv (c_msg, consumer);
    assert (dafka_consumer_msg_streq (c_msg, "2"));

    dafka_consumer_msg_recv (c_msg, consumer);
    assert (dafka_consumer_msg_streq (c_msg, "3"));

    dafka_consumer_msg_destroy (&c_msg);
    dafka_consumer_destroy (&consumer);
    dafka_producer_msg_destroy (&p_msg);
    zactor_destroy (&store);
    zactor_destroy (&tower);
    zconfig_destroy (&config);

    // ----------------------------------------------------
    //  Cleanup test artifacts
    // ----------------------------------------------------
    zdir_t *store_dir = zdir_new (SELFTEST_DIR_RW "/storedb", NULL);
    zdir_remove (store_dir, true);
    zdir_destroy (&store_dir);
    //  @end

    printf ("OK\n");
}
