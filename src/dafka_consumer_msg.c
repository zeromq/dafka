/*  =========================================================================
    dafka_consumer_msg - class description

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
    dafka_consumer_msg -
@discuss
@end
*/

#include "dafka_classes.h"

//  Structure of our class

struct _dafka_consumer_msg_t {
    char *subject;
    char *address;
    zframe_t *content;
};


//  --------------------------------------------------------------------------
//  Create a new dafka_consumer_msg

dafka_consumer_msg_t *
dafka_consumer_msg_new (void)
{
    dafka_consumer_msg_t *self = (dafka_consumer_msg_t *) zmalloc (sizeof (dafka_consumer_msg_t));
    assert (self);
    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_consumer_msg

void
dafka_consumer_msg_destroy (dafka_consumer_msg_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_consumer_msg_t *self = *self_p;
        zframe_destroy (&self->content);

        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}

//  Return the subject of the msg.
const char *
dafka_consumer_msg_subject (dafka_consumer_msg_t *self) {
    assert (self);

    return self->subject;
}

//  Return the sender address of the msg.
const char *
dafka_consumer_msg_address (dafka_consumer_msg_t *self) {
    assert (self);

    return self->address;
}

//  Return the content of the msg.
//  Content buffer is belong to the msg.
const byte *
dafka_consumer_msg_content (dafka_consumer_msg_t *self) {
    assert (self);

    if (self->content == NULL)
        return NULL;

    return zframe_data (self->content);
}

//  Return the size of the content
DAFKA_EXPORT size_t
dafka_consumer_msg_content_size (dafka_consumer_msg_t *self) {
    assert (self);

    if (self->content == NULL)
        return 0;

    return zframe_size (self->content);
}

//  Return the content, user takes ownership on the frame returned.
//  Caller owns return value and must destroy it when done.
zframe_t *
dafka_consumer_msg_get_content (dafka_consumer_msg_t *self) {
    assert (self);

    zframe_t *content = self->content;
    self->content = NULL;

    return content;
}

//  Receive a msg from a consumer actor.
//  Return 0 on success and -1 on error.
DAFKA_EXPORT int
dafka_consumer_msg_recv (dafka_consumer_msg_t *self, zactor_t * consumer) {
    assert (self);

    zframe_destroy (&self->content);

    return zsock_brecv (consumer, "ssf", &self->subject, &self->address, &self->content);
}


//  Return frame data copied into freshly allocated string
//  Caller must free string when finished with it.
//  Caller owns return value and must destroy it when done.
char *
dafka_consumer_msg_strdup (dafka_consumer_msg_t *self) {
    assert (self);

    if (self->content == NULL)
        return NULL;

    return zframe_strdup (self->content);
}

//  Return TRUE if content is equal to string, excluding terminator
bool
dafka_consumer_msg_streq (dafka_consumer_msg_t *self, const char *string) {
    assert (self);
    assert (string);

    if (self->content == NULL)
        return false;

    return zframe_streq (self->content, string);
}



//  --------------------------------------------------------------------------
//  Self test of this class

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
dafka_consumer_msg_test (bool verbose)
{
    printf (" * dafka_consumer_msg: ");

    //  @selftest
    //  Simple create/destroy test
    dafka_consumer_msg_t *self = dafka_consumer_msg_new ();
    assert (self);
    dafka_consumer_msg_destroy (&self);
    //  @end
    printf ("OK\n");
}
