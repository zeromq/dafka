/*  =========================================================================
    dafka_proto - Set the content field, transferring ownership from caller

    Copyright (c) the Contributors as noted in the AUTHORS file.
    This file is part of CZMQ, the high-level C binding for 0MQ:
    http://czmq.zeromq.org.

    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
    =========================================================================
*/

#ifndef DAFKA_PROTO_H_INCLUDED
#define DAFKA_PROTO_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

//  @warning THE FOLLOWING @INTERFACE BLOCK IS AUTO-GENERATED BY ZPROJECT
//  @warning Please edit the model at "api/dafka_proto.api" to make changes.
//  @interface
//  This is a stable class, and may not change except for emergencies. It
//  is provided in stable builds.
#define DAFKA_PROTO_MSG 'M'                  //
#define DAFKA_PROTO_RELIABLE 'R'             //
#define DAFKA_PROTO_ASK 'A'                  //
#define DAFKA_PROTO_ANSWER 'W'               //

//  Create a new empty dafka_proto
DAFKA_EXPORT dafka_proto_t *
    dafka_proto_new (void);

//  Create a new dafka_proto from zpl/zconfig_t *
DAFKA_EXPORT dafka_proto_t *
    dafka_proto_new_zpl (zconfig_t *config);

//  Destroy a dafka_proto instance
DAFKA_EXPORT void
    dafka_proto_destroy (dafka_proto_t **self_p);

//  Create a deep copy of a dafka_proto instance
//  Caller owns return value and must destroy it when done.
DAFKA_EXPORT dafka_proto_t *
    dafka_proto_dup (dafka_proto_t *self);

//  Receive a dafka_proto from the socket. Returns 0 if OK, -1 if
//  there was an error. Blocks if there is no message waiting.
DAFKA_EXPORT int
    dafka_proto_recv (dafka_proto_t *self, zsock_t *input);

//  Send the dafka_proto to the output socket, does not destroy it
DAFKA_EXPORT int
    dafka_proto_send (dafka_proto_t *self, zsock_t *output);

//  Print contents of message to stdout
DAFKA_EXPORT void
    dafka_proto_print (dafka_proto_t *self);

//  Export class as zconfig_t*. Caller is responsibe for destroying the instance
//  Caller owns return value and must destroy it when done.
DAFKA_EXPORT zconfig_t *
    dafka_proto_zpl (dafka_proto_t *self, zconfig_t *parent);

//  Get the message routing id, as a frame
DAFKA_EXPORT zframe_t *
    dafka_proto_routing_id (dafka_proto_t *self);

//  Set the message routing id from a frame
DAFKA_EXPORT void
    dafka_proto_set_routing_id (dafka_proto_t *self, zframe_t *routing_id);

//  Get the topic of the message for publishing over pub/sub
DAFKA_EXPORT const char *
    dafka_proto_topic (dafka_proto_t *self);

//  Set the topic of the message for publishing over pub/sub
DAFKA_EXPORT void
    dafka_proto_set_topic (dafka_proto_t *self, const char *topic);

//  Subscribe a socket to a specific message id and a topic.
DAFKA_EXPORT void
    dafka_proto_subscribe (zsock_t *sub, char id, const char *topic);

//  Unsubscribe a socket form a specific message id and a topic.
DAFKA_EXPORT void
    dafka_proto_unsubscribe (zsock_t *sub, char id, const char *topic);

//  Get the dafka_proto message id
DAFKA_EXPORT char
    dafka_proto_id (dafka_proto_t *self);

//  Set the dafka_proto message id
DAFKA_EXPORT void
    dafka_proto_set_id (dafka_proto_t *self, char id);

//  Get the dafka_proto message id as printable text
DAFKA_EXPORT const char *
    dafka_proto_command (dafka_proto_t *self);

//  Get a copy of the content field
DAFKA_EXPORT zframe_t *
    dafka_proto_content (dafka_proto_t *self);

//  Get the content field and transfer ownership to caller
DAFKA_EXPORT zframe_t *
    dafka_proto_get_content (dafka_proto_t *self);

//
DAFKA_EXPORT void
    dafka_proto_set_content (dafka_proto_t *self, zframe_t **content_p);

//  Get the address field
DAFKA_EXPORT const char *
    dafka_proto_address (dafka_proto_t *self);

//  Set the address field
DAFKA_EXPORT void
    dafka_proto_set_address (dafka_proto_t *self, const char *address);

//  Get the sequence field
DAFKA_EXPORT uint64_t
    dafka_proto_sequence (dafka_proto_t *self);

//  Set the sequence field
DAFKA_EXPORT void
    dafka_proto_set_sequence (dafka_proto_t *self, uint64_t sequence);

//  Get the subject field
DAFKA_EXPORT const char *
    dafka_proto_subject (dafka_proto_t *self);

//  Set the subject field
DAFKA_EXPORT void
    dafka_proto_set_subject (dafka_proto_t *self, const char *subject);

//  Get the begin_offset field
DAFKA_EXPORT uint64_t
    dafka_proto_begin_offset (dafka_proto_t *self);

//  Set the begin_offset field
DAFKA_EXPORT void
    dafka_proto_set_begin_offset (dafka_proto_t *self, uint64_t begin_offset);

//  Get the end_offset field
DAFKA_EXPORT uint64_t
    dafka_proto_end_offset (dafka_proto_t *self);

//  Set the end_offset field
DAFKA_EXPORT void
    dafka_proto_set_end_offset (dafka_proto_t *self, uint64_t end_offset);

//  Self test of this class.
DAFKA_EXPORT void
    dafka_proto_test (bool verbose);

//  @end

#ifdef __cplusplus
}
#endif

#endif
