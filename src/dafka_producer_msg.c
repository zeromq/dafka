/*  =========================================================================
    dafka_producer_msg - class description

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
    dafka_producer_msg -
@discuss
@end
*/

#include "dafka_classes.h"

//  Structure of our class

struct _dafka_producer_msg_t {
    zframe_t *content;
};


//  --------------------------------------------------------------------------
//  Create a new dafka_producer_msg

dafka_producer_msg_t *
dafka_producer_msg_new (void)
{
    dafka_producer_msg_t *self = (dafka_producer_msg_t *) zmalloc (sizeof (dafka_producer_msg_t));
    assert (self);
    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_producer_msg

void
dafka_producer_msg_destroy (dafka_producer_msg_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_producer_msg_t *self = *self_p;
        zframe_destroy (&self->content);

        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}


//  Return the content of the record.
//  Content buffer is belong to the record.
const byte *
dafka_producer_msg_content (dafka_producer_msg_t *self) {
    assert (self);

    if (self->content == NULL)
        return NULL;

    return zframe_data (self->content);
}

//  Return the size of the content
size_t
dafka_producer_msg_content_size (dafka_producer_msg_t *self) {
    assert (self);

    if (self->content == NULL)
        return 0;

    return zframe_size (self->content);
}

//  Return the content, user takes ownership of the frame returned.
//  Caller owns return value and must destroy it when done.
zframe_t *
dafka_producer_msg_get_content (dafka_producer_msg_t *self) {
    assert (self);

    zframe_t *content = self->content;
    self->content = NULL;

    return content;
}

//  Create a new content buffer at the specific size.
int
dafka_producer_msg_init_content (dafka_producer_msg_t *self, size_t size) {
    assert (self);
    zframe_destroy (&self->content);

    self->content = zframe_new (NULL, size);

    return self->content ? 0 : 1;
}

//  Set the content with a frame. Takes ownership on the content.
DAFKA_EXPORT void
dafka_producer_msg_set_content (dafka_producer_msg_t *self, zframe_t **content) {
    assert (self);
    assert (content);
    zframe_destroy (&self->content);

    self->content = *content;
    *content = NULL;
}

//  Set the content from string.
int
dafka_producer_msg_set_content_str (dafka_producer_msg_t *self, const char *content) {
    assert (self);
    assert (content);

    zframe_destroy (&self->content);

    size_t size = strlen (content);
    self->content = zframe_new (content, size);

    return self->content ? 0 : 1;
}

//  Set the content from string.
//  Return 0 on success and -1 if not enough memory.
int
dafka_producer_msg_set_content_buffer (dafka_producer_msg_t *self, const byte *content, size_t content_size) {
    assert (self);
    assert (content);

    zframe_destroy (&self->content);
    self->content = zframe_new (content, content_size);

    return self->content ? 0 : 1;
}


//  Send a record to producer.
//  Content will ownership will be moved to a background thread and will be set to NULL.
//  Return 0 on success and -1 on error.
int
dafka_producer_msg_send (dafka_producer_msg_t *self, zactor_t *producer) {
    assert (self);
    assert (self->content);

    int rc = zstr_sendm (producer, "P");
    if (rc == -1)
        return -1;

    zframe_send (&self->content, producer, 0);

    return 0;
}

//  Return frame data copied into freshly allocated string
//  Caller must free string when finished with it.
//  Caller owns return value and must destroy it when done.
char *
dafka_producer_msg_strdup (dafka_producer_msg_t *self) {
    assert (self);

    if (self->content == NULL)
        return NULL;

    return zframe_strdup (self->content);
}

//  Return TRUE if content is equal to string, excluding terminator
bool
dafka_producer_msg_streq (dafka_producer_msg_t *self, const char *string) {
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
dafka_producer_msg_test (bool verbose)
{
    printf (" * dafka_producer_msg: ");

    //  @selftest
    //  Simple create/destroy test
    dafka_producer_msg_t *self = dafka_producer_msg_new ();
    assert (self);
    dafka_producer_msg_destroy (&self);
    //  @end
    printf ("OK\n");
}
