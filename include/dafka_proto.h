/*  =========================================================================
    dafka_proto - dafka_proto

    Codec header for dafka_proto.

    ** WARNING *************************************************************
    THIS SOURCE FILE IS 100% GENERATED. If you edit this file, you will lose
    your changes at the next build cycle. This is great for temporary printf
    statements. DO NOT MAKE ANY CHANGES YOU WISH TO KEEP. The correct places
    for commits are:

     * The XML model used for this code generation: dafka_proto.xml, or
     * The code generation script that built this file: zproto_codec_c
    ************************************************************************
    =========================================================================
*/

#ifndef DAFKA_PROTO_H_INCLUDED
#define DAFKA_PROTO_H_INCLUDED

/*  These are the dafka_proto messages:

    MSG -
        topic               string

    RELIABLE -
        topic               string
        address             string
        sequence            number 8
        content             frame

    ASK -
        topic               string
        subject             string
        begin_offset        number 8
        end_offset          number 8
        address             string

    ANSWER -
        topic               string
        subject             string
        sender              string
        sequence            number 8
        content             frame
*/


#define DAFKA_PROTO_MSG                     'm'
#define DAFKA_PROTO_RELIABLE                'r'
#define DAFKA_PROTO_ASK                     'a'
#define DAFKA_PROTO_ANSWER                  'w'

#include <czmq.h>

#ifdef __cplusplus
extern "C" {
#endif

//  Opaque class structure
#ifndef DAFKA_PROTO_T_DEFINED
typedef struct _dafka_proto_t dafka_proto_t;
#define DAFKA_PROTO_T_DEFINED
#endif

//  @interface
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
DAFKA_EXPORT dafka_proto_t *
    dafka_proto_dup (dafka_proto_t *other);

//  Receive a dafka_proto from the socket. Returns 0 if OK, -1 if
//  the read was interrupted, or -2 if the message is malformed.
//  Blocks if there is no message waiting.
DAFKA_EXPORT int
    dafka_proto_recv (dafka_proto_t *self, zsock_t *input);

//  Send the dafka_proto to the output socket, does not destroy it
DAFKA_EXPORT int
    dafka_proto_send (dafka_proto_t *self, zsock_t *output);


//  Print contents of message to stdout
DAFKA_EXPORT void
    dafka_proto_print (dafka_proto_t *self);

//  Export class as zconfig_t*. Caller is responsibe for destroying the instance
DAFKA_EXPORT zconfig_t *
    dafka_proto_zpl (dafka_proto_t *self, zconfig_t* parent);

//  Get/set the message routing id
DAFKA_EXPORT zframe_t *
    dafka_proto_routing_id (dafka_proto_t *self);
DAFKA_EXPORT void
    dafka_proto_set_routing_id (dafka_proto_t *self, zframe_t *routing_id);

//  Get the dafka_proto id and printable command
DAFKA_EXPORT int
    dafka_proto_id (dafka_proto_t *self);
DAFKA_EXPORT void
    dafka_proto_set_id (dafka_proto_t *self, int id);
DAFKA_EXPORT const char *
    dafka_proto_command (dafka_proto_t *self);

//  Get/set the topic field
DAFKA_EXPORT const char *
    dafka_proto_topic (dafka_proto_t *self);
DAFKA_EXPORT void
    dafka_proto_set_topic (dafka_proto_t *self, const char *value);

//  Get/set the address field
DAFKA_EXPORT const char *
    dafka_proto_address (dafka_proto_t *self);
DAFKA_EXPORT void
    dafka_proto_set_address (dafka_proto_t *self, const char *value);

//  Get/set the sequence field
DAFKA_EXPORT uint64_t
    dafka_proto_sequence (dafka_proto_t *self);
DAFKA_EXPORT void
    dafka_proto_set_sequence (dafka_proto_t *self, uint64_t sequence);

//  Get a copy of the content field
DAFKA_EXPORT zframe_t *
    dafka_proto_content (dafka_proto_t *self);
//  Get the content field and transfer ownership to caller
DAFKA_EXPORT zframe_t *
    dafka_proto_get_content (dafka_proto_t *self);
//  Set the content field, transferring ownership from caller
DAFKA_EXPORT void
    dafka_proto_set_content (dafka_proto_t *self, zframe_t **frame_p);

//  Get/set the subject field
DAFKA_EXPORT const char *
    dafka_proto_subject (dafka_proto_t *self);
DAFKA_EXPORT void
    dafka_proto_set_subject (dafka_proto_t *self, const char *value);

//  Get/set the begin_offset field
DAFKA_EXPORT uint64_t
    dafka_proto_begin_offset (dafka_proto_t *self);
DAFKA_EXPORT void
    dafka_proto_set_begin_offset (dafka_proto_t *self, uint64_t begin_offset);

//  Get/set the end_offset field
DAFKA_EXPORT uint64_t
    dafka_proto_end_offset (dafka_proto_t *self);
DAFKA_EXPORT void
    dafka_proto_set_end_offset (dafka_proto_t *self, uint64_t end_offset);

//  Get/set the sender field
DAFKA_EXPORT const char *
    dafka_proto_sender (dafka_proto_t *self);
DAFKA_EXPORT void
    dafka_proto_set_sender (dafka_proto_t *self, const char *value);

//  Self test of this class
DAFKA_EXPORT void
    dafka_proto_test (bool verbose);
//  @end

//  For backwards compatibility with old codecs
#define dafka_proto_dump    dafka_proto_print

#ifdef __cplusplus
}
#endif

#endif
