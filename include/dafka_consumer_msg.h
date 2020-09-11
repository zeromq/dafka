/*  =========================================================================
    dafka_consumer_record - class description

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_CONSUMER_RECORD_H_INCLUDED
#define DAFKA_CONSUMER_RECORD_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

//  @warning THE FOLLOWING @INTERFACE BLOCK IS AUTO-GENERATED BY ZPROJECT
//  @warning Please edit the model at "api/dafka_consumer_msg.api" to make changes.
//  @interface
//  This is a stable class, and may not change except for emergencies. It
//  is provided in stable builds.
//  Create a new dafka_consumer_msg.
DAFKA_EXPORT dafka_consumer_msg_t *
    dafka_consumer_msg_new (void);

//  Destroy the dafka_consumer_msg.
DAFKA_EXPORT void
    dafka_consumer_msg_destroy (dafka_consumer_msg_t **self_p);

//  Return the subject of the message.
DAFKA_EXPORT const char *
    dafka_consumer_msg_subject (dafka_consumer_msg_t *self);

//  Return the sender address of the message.
DAFKA_EXPORT const char *
    dafka_consumer_msg_address (dafka_consumer_msg_t *self);

//  Return the content of the message.
//  Content buffer is belong to the message.
DAFKA_EXPORT const byte *
    dafka_consumer_msg_content (dafka_consumer_msg_t *self);

//  Return the size of the content
DAFKA_EXPORT size_t
    dafka_consumer_msg_content_size (dafka_consumer_msg_t *self);

//  Return the content, user takes ownership of the frame returned.
//  Caller owns return value and must destroy it when done.
DAFKA_EXPORT zframe_t *
    dafka_consumer_msg_get_content (dafka_consumer_msg_t *self);

//  Receive a message from a consumer actor.
//  Return 0 on success and -1 on error.
DAFKA_EXPORT int
    dafka_consumer_msg_recv (dafka_consumer_msg_t *self, dafka_consumer_t *consumer);

//  Receive a message from a custom socket.
//  Return 0 on success and -1 on error.
DAFKA_EXPORT int
    dafka_consumer_msg_recv_from_socket (dafka_consumer_msg_t *self, zsock_t *socket);

//  Return frame data copied into freshly allocated string
//  Caller must free string when finished with it.
//  Caller owns return value and must destroy it when done.
DAFKA_EXPORT char *
    dafka_consumer_msg_strdup (dafka_consumer_msg_t *self);

//  Return TRUE if content is equal to string, excluding terminator
DAFKA_EXPORT bool
    dafka_consumer_msg_streq (dafka_consumer_msg_t *self, const char *string);

//  Self test of this class.
DAFKA_EXPORT void
    dafka_consumer_msg_test (bool verbose);

//  @end

#ifdef __cplusplus
}
#endif

#endif
