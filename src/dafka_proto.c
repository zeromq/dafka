/*  =========================================================================
    dafka_proto - dafka_proto

    Codec class for dafka_proto.

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

/*
@header
    dafka_proto - dafka_proto
@discuss
@end
*/

#ifdef NDEBUG
#undef NDEBUG
#endif

#include "dafka_classes.h"
#include "../include/dafka_proto.h"

//  Structure of our class

struct _dafka_proto_t {
    zframe_t *routing_id;               //  Routing_id from ROUTER, if any
    char id;                            //  dafka_proto message ID
    char *topic;                        //  Topic to send and receive over pub/sub
    bool is_subscribe;                  //  Indicate if it a subscribe or unsubscribe command
    byte *needle;                       //  Read/write pointer for serialization
    byte *ceiling;                      //  Valid upper limit for read pointer
    char address [256];                 //  address
    char subject [256];                 //  subject
    uint64_t sequence;                  //  sequence
    zmq_msg_t content;                  //  content
    uint32_t count;                     //  count
    zlist_t *subjects;                  //  subjects
};

//  --------------------------------------------------------------------------
//  Network data encoding macros

//  Put a block of octets to the frame
#define PUT_OCTETS(host,size) { \
    memcpy (self->needle, (host), size); \
    self->needle += size; \
}

//  Get a block of octets from the frame
#define GET_OCTETS(host,size) { \
    if (self->needle + size > self->ceiling) { \
        zsys_warning ("dafka_proto: GET_OCTETS failed"); \
        goto malformed; \
    } \
    memcpy ((host), self->needle, size); \
    self->needle += size; \
}

//  Put a 1-byte number to the frame
#define PUT_NUMBER1(host) { \
    *(byte *) self->needle = (byte) (host); \
    self->needle++; \
}

//  Put a 2-byte number to the frame
#define PUT_NUMBER2(host) { \
    self->needle [0] = (byte) (((host) >> 8)  & 255); \
    self->needle [1] = (byte) (((host))       & 255); \
    self->needle += 2; \
}

//  Put a 4-byte number to the frame
#define PUT_NUMBER4(host) { \
    self->needle [0] = (byte) (((host) >> 24) & 255); \
    self->needle [1] = (byte) (((host) >> 16) & 255); \
    self->needle [2] = (byte) (((host) >> 8)  & 255); \
    self->needle [3] = (byte) (((host))       & 255); \
    self->needle += 4; \
}

//  Put a 8-byte number to the frame
#define PUT_NUMBER8(host) { \
    self->needle [0] = (byte) (((host) >> 56) & 255); \
    self->needle [1] = (byte) (((host) >> 48) & 255); \
    self->needle [2] = (byte) (((host) >> 40) & 255); \
    self->needle [3] = (byte) (((host) >> 32) & 255); \
    self->needle [4] = (byte) (((host) >> 24) & 255); \
    self->needle [5] = (byte) (((host) >> 16) & 255); \
    self->needle [6] = (byte) (((host) >> 8)  & 255); \
    self->needle [7] = (byte) (((host))       & 255); \
    self->needle += 8; \
}

//  Get a 1-byte number from the frame
#define GET_NUMBER1(host) { \
    if (self->needle + 1 > self->ceiling) { \
        zsys_warning ("dafka_proto: GET_NUMBER1 failed"); \
        goto malformed; \
    } \
    (host) = *(byte *) self->needle; \
    self->needle++; \
}

//  Get a 2-byte number from the frame
#define GET_NUMBER2(host) { \
    if (self->needle + 2 > self->ceiling) { \
        zsys_warning ("dafka_proto: GET_NUMBER2 failed"); \
        goto malformed; \
    } \
    (host) = ((uint16_t) (self->needle [0]) << 8) \
           +  (uint16_t) (self->needle [1]); \
    self->needle += 2; \
}

//  Get a 4-byte number from the frame
#define GET_NUMBER4(host) { \
    if (self->needle + 4 > self->ceiling) { \
        zsys_warning ("dafka_proto: GET_NUMBER4 failed"); \
        goto malformed; \
    } \
    (host) = ((uint32_t) (self->needle [0]) << 24) \
           + ((uint32_t) (self->needle [1]) << 16) \
           + ((uint32_t) (self->needle [2]) << 8) \
           +  (uint32_t) (self->needle [3]); \
    self->needle += 4; \
}

//  Get a 8-byte number from the frame
#define GET_NUMBER8(host) { \
    if (self->needle + 8 > self->ceiling) { \
        zsys_warning ("dafka_proto: GET_NUMBER8 failed"); \
        goto malformed; \
    } \
    (host) = ((uint64_t) (self->needle [0]) << 56) \
           + ((uint64_t) (self->needle [1]) << 48) \
           + ((uint64_t) (self->needle [2]) << 40) \
           + ((uint64_t) (self->needle [3]) << 32) \
           + ((uint64_t) (self->needle [4]) << 24) \
           + ((uint64_t) (self->needle [5]) << 16) \
           + ((uint64_t) (self->needle [6]) << 8) \
           +  (uint64_t) (self->needle [7]); \
    self->needle += 8; \
}

//  Put a string to the frame
#define PUT_STRING(host) { \
    size_t string_size = strlen (host); \
    PUT_NUMBER1 (string_size); \
    memcpy (self->needle, (host), string_size); \
    self->needle += string_size; \
}

//  Get a string from the frame
#define GET_STRING(host) { \
    size_t string_size; \
    GET_NUMBER1 (string_size); \
    if (self->needle + string_size > (self->ceiling)) { \
        zsys_warning ("dafka_proto: GET_STRING failed"); \
        goto malformed; \
    } \
    memcpy ((host), self->needle, string_size); \
    (host) [string_size] = 0; \
    self->needle += string_size; \
}

//  Put a long string to the frame
#define PUT_LONGSTR(host) { \
    size_t string_size = strlen (host); \
    PUT_NUMBER4 (string_size); \
    memcpy (self->needle, (host), string_size); \
    self->needle += string_size; \
}

//  Get a long string from the frame
#define GET_LONGSTR(host) { \
    size_t string_size; \
    GET_NUMBER4 (string_size); \
    if (self->needle + string_size > (self->ceiling)) { \
        zsys_warning ("dafka_proto: GET_LONGSTR failed"); \
        goto malformed; \
    } \
    free ((host)); \
    (host) = (char *) malloc (string_size + 1); \
    memcpy ((host), self->needle, string_size); \
    (host) [string_size] = 0; \
    self->needle += string_size; \
}

//  --------------------------------------------------------------------------
//  bytes cstring conversion macros

// create new array of unsigned char from properly encoded string
// len of resulting array is strlen (str) / 2
// caller is responsibe for freeing up the memory
#define BYTES_FROM_STR(dst, _str) { \
    char *str = (char*) (_str); \
    if (!str || (strlen (str) % 2) != 0) \
        return NULL; \
\
    size_t strlen_2 = strlen (str) / 2; \
    byte *mem = (byte*) zmalloc (strlen_2); \
    size_t i; \
\
    for (i = 0; i != strlen_2; i++) \
    { \
        char buff[3] = {0x0, 0x0, 0x0}; \
        strncpy (buff, str, 2); \
        unsigned int uint; \
        sscanf (buff, "%x", &uint); \
        assert (uint <= 0xff); \
        mem [i] = (byte) (0xff & uint); \
        str += 2; \
    } \
    dst = mem; \
}

// convert len bytes to hex string
// caller is responsibe for freeing up the memory
#define STR_FROM_BYTES(dst, _mem, _len) { \
    byte *mem = (byte*) (_mem); \
    size_t len = (size_t) (_len); \
    char* ret = (char*) zmalloc (2*len + 1); \
    char* aux = ret; \
    size_t i; \
    for (i = 0; i != len; i++) \
    { \
        sprintf (aux, "%02x", mem [i]); \
        aux+=2; \
    } \
    dst = ret; \
}

//  --------------------------------------------------------------------------
//  Create a new dafka_proto

dafka_proto_t *
dafka_proto_new (void)
{
    dafka_proto_t *self = (dafka_proto_t *) zmalloc (sizeof (dafka_proto_t));
    zmq_msg_init (&self->content);
    return self;
}

//  --------------------------------------------------------------------------
//  Create a new dafka_proto from zpl/zconfig_t *

dafka_proto_t *
    dafka_proto_new_zpl (zconfig_t *config)
{
    assert (config);
    dafka_proto_t *self = NULL;
    char *message = zconfig_get (config, "message", NULL);
    if (!message) {
        zsys_error ("Can't find 'message' section");
        return NULL;
    }

    if (streq ("DAFKA_PROTO_RECORD", message)) {
        self = dafka_proto_new ();
        dafka_proto_set_id (self, DAFKA_PROTO_RECORD);
    }
    else
    if (streq ("DAFKA_PROTO_DIRECT_RECORD", message)) {
        self = dafka_proto_new ();
        dafka_proto_set_id (self, DAFKA_PROTO_DIRECT_RECORD);
    }
    else
    if (streq ("DAFKA_PROTO_FETCH", message)) {
        self = dafka_proto_new ();
        dafka_proto_set_id (self, DAFKA_PROTO_FETCH);
    }
    else
    if (streq ("DAFKA_PROTO_ACK", message)) {
        self = dafka_proto_new ();
        dafka_proto_set_id (self, DAFKA_PROTO_ACK);
    }
    else
    if (streq ("DAFKA_PROTO_HEAD", message)) {
        self = dafka_proto_new ();
        dafka_proto_set_id (self, DAFKA_PROTO_HEAD);
    }
    else
    if (streq ("DAFKA_PROTO_DIRECT_HEAD", message)) {
        self = dafka_proto_new ();
        dafka_proto_set_id (self, DAFKA_PROTO_DIRECT_HEAD);
    }
    else
    if (streq ("DAFKA_PROTO_GET_HEADS", message)) {
        self = dafka_proto_new ();
        dafka_proto_set_id (self, DAFKA_PROTO_GET_HEADS);
    }
    else
    if (streq ("DAFKA_PROTO_CONSUMER_HELLO", message)) {
        self = dafka_proto_new ();
        dafka_proto_set_id (self, DAFKA_PROTO_CONSUMER_HELLO);
    }
    else
    if (streq ("DAFKA_PROTO_STORE_HELLO", message)) {
        self = dafka_proto_new ();
        dafka_proto_set_id (self, DAFKA_PROTO_STORE_HELLO);
    }
    else
       {
        zsys_error ("message=%s is not known", message);
        return NULL;
       }

    char *s = zconfig_get (config, "routing_id", NULL);
    if (s) {
        byte *bvalue;
        BYTES_FROM_STR (bvalue, s);
        if (!bvalue) {
            dafka_proto_destroy (&self);
            return NULL;
        }
        zframe_t *frame = zframe_new (bvalue, strlen (s) / 2);
        free (bvalue);
        self->routing_id = frame;
    }

    char *topic = zconfig_get (config, "topic", NULL);
    if (topic) {
        zstr_free (&self->topic);
        self->topic = strdup (topic);
    }

    zconfig_t *content = NULL;
    switch (self->id) {
        case DAFKA_PROTO_RECORD:
            content = zconfig_locate (config, "content");
            if (!content) {
                zsys_error ("Can't find 'content' section");
                dafka_proto_destroy (&self);
                return NULL;
            }
            {
            char *s = zconfig_get (content, "address", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->address, s, 255);
            }
            {
            char *s = zconfig_get (content, "subject", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->subject, s, 255);
            }
            {
            char *es = NULL;
            char *s = zconfig_get (content, "sequence", NULL);
            if (!s) {
                zsys_error ("content/sequence not found");
                dafka_proto_destroy (&self);
                return NULL;
            }
            uint64_t uvalue = (uint64_t) strtoll (s, &es, 10);
            if (es != s+strlen (s)) {
                zsys_error ("content/sequence: %s is not a number", s);
                dafka_proto_destroy (&self);
                return NULL;
            }
            self->sequence = uvalue;
            }
            {
            char *s = zconfig_get (content, "content", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            byte *bvalue;
            BYTES_FROM_STR (bvalue, s);
            if (!bvalue) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            zmq_msg_close (&self->content);
            zmq_msg_init_size (&self->content, strlen (s) / 2);
            memcpy (zmq_msg_data (&self->content), bvalue, zmq_msg_size (&self->content));
            free (bvalue);
            }

            break;
        case DAFKA_PROTO_DIRECT_RECORD:
            content = zconfig_locate (config, "content");
            if (!content) {
                zsys_error ("Can't find 'content' section");
                dafka_proto_destroy (&self);
                return NULL;
            }
            {
            char *s = zconfig_get (content, "address", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->address, s, 255);
            }
            {
            char *s = zconfig_get (content, "subject", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->subject, s, 255);
            }
            {
            char *es = NULL;
            char *s = zconfig_get (content, "sequence", NULL);
            if (!s) {
                zsys_error ("content/sequence not found");
                dafka_proto_destroy (&self);
                return NULL;
            }
            uint64_t uvalue = (uint64_t) strtoll (s, &es, 10);
            if (es != s+strlen (s)) {
                zsys_error ("content/sequence: %s is not a number", s);
                dafka_proto_destroy (&self);
                return NULL;
            }
            self->sequence = uvalue;
            }
            {
            char *s = zconfig_get (content, "content", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            byte *bvalue;
            BYTES_FROM_STR (bvalue, s);
            if (!bvalue) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            zmq_msg_close (&self->content);
            zmq_msg_init_size (&self->content, strlen (s) / 2);
            memcpy (zmq_msg_data (&self->content), bvalue, zmq_msg_size (&self->content));
            free (bvalue);
            }

            break;
        case DAFKA_PROTO_FETCH:
            content = zconfig_locate (config, "content");
            if (!content) {
                zsys_error ("Can't find 'content' section");
                dafka_proto_destroy (&self);
                return NULL;
            }
            {
            char *s = zconfig_get (content, "address", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->address, s, 255);
            }
            {
            char *s = zconfig_get (content, "subject", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->subject, s, 255);
            }
            {
            char *es = NULL;
            char *s = zconfig_get (content, "sequence", NULL);
            if (!s) {
                zsys_error ("content/sequence not found");
                dafka_proto_destroy (&self);
                return NULL;
            }
            uint64_t uvalue = (uint64_t) strtoll (s, &es, 10);
            if (es != s+strlen (s)) {
                zsys_error ("content/sequence: %s is not a number", s);
                dafka_proto_destroy (&self);
                return NULL;
            }
            self->sequence = uvalue;
            }
            {
            char *es = NULL;
            char *s = zconfig_get (content, "count", NULL);
            if (!s) {
                zsys_error ("content/count not found");
                dafka_proto_destroy (&self);
                return NULL;
            }
            uint64_t uvalue = (uint64_t) strtoll (s, &es, 10);
            if (es != s+strlen (s)) {
                zsys_error ("content/count: %s is not a number", s);
                dafka_proto_destroy (&self);
                return NULL;
            }
            self->count = uvalue;
            }
            break;
        case DAFKA_PROTO_ACK:
            content = zconfig_locate (config, "content");
            if (!content) {
                zsys_error ("Can't find 'content' section");
                dafka_proto_destroy (&self);
                return NULL;
            }
            {
            char *s = zconfig_get (content, "subject", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->subject, s, 255);
            }
            {
            char *es = NULL;
            char *s = zconfig_get (content, "sequence", NULL);
            if (!s) {
                zsys_error ("content/sequence not found");
                dafka_proto_destroy (&self);
                return NULL;
            }
            uint64_t uvalue = (uint64_t) strtoll (s, &es, 10);
            if (es != s+strlen (s)) {
                zsys_error ("content/sequence: %s is not a number", s);
                dafka_proto_destroy (&self);
                return NULL;
            }
            self->sequence = uvalue;
            }
            break;
        case DAFKA_PROTO_HEAD:
            content = zconfig_locate (config, "content");
            if (!content) {
                zsys_error ("Can't find 'content' section");
                dafka_proto_destroy (&self);
                return NULL;
            }
            {
            char *s = zconfig_get (content, "address", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->address, s, 255);
            }
            {
            char *s = zconfig_get (content, "subject", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->subject, s, 255);
            }
            {
            char *es = NULL;
            char *s = zconfig_get (content, "sequence", NULL);
            if (!s) {
                zsys_error ("content/sequence not found");
                dafka_proto_destroy (&self);
                return NULL;
            }
            uint64_t uvalue = (uint64_t) strtoll (s, &es, 10);
            if (es != s+strlen (s)) {
                zsys_error ("content/sequence: %s is not a number", s);
                dafka_proto_destroy (&self);
                return NULL;
            }
            self->sequence = uvalue;
            }
            break;
        case DAFKA_PROTO_DIRECT_HEAD:
            content = zconfig_locate (config, "content");
            if (!content) {
                zsys_error ("Can't find 'content' section");
                dafka_proto_destroy (&self);
                return NULL;
            }
            {
            char *s = zconfig_get (content, "address", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->address, s, 255);
            }
            {
            char *s = zconfig_get (content, "subject", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->subject, s, 255);
            }
            {
            char *es = NULL;
            char *s = zconfig_get (content, "sequence", NULL);
            if (!s) {
                zsys_error ("content/sequence not found");
                dafka_proto_destroy (&self);
                return NULL;
            }
            uint64_t uvalue = (uint64_t) strtoll (s, &es, 10);
            if (es != s+strlen (s)) {
                zsys_error ("content/sequence: %s is not a number", s);
                dafka_proto_destroy (&self);
                return NULL;
            }
            self->sequence = uvalue;
            }
            break;
        case DAFKA_PROTO_GET_HEADS:
            content = zconfig_locate (config, "content");
            if (!content) {
                zsys_error ("Can't find 'content' section");
                dafka_proto_destroy (&self);
                return NULL;
            }
            {
            char *s = zconfig_get (content, "address", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->address, s, 255);
            }
            break;
        case DAFKA_PROTO_CONSUMER_HELLO:
            content = zconfig_locate (config, "content");
            if (!content) {
                zsys_error ("Can't find 'content' section");
                dafka_proto_destroy (&self);
                return NULL;
            }
            {
            char *s = zconfig_get (content, "address", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->address, s, 255);
            }
            {
            zconfig_t *zstrings = zconfig_locate (content, "subjects");
            if (zstrings) {
                zlist_t *strings = zlist_new ();
                zlist_autofree (strings);
                zconfig_t *child;
                for (child = zconfig_child (zstrings);
                                child != NULL;
                                child = zconfig_next (child))
                {
                    zlist_append (strings, zconfig_value (child));
                }
                self->subjects = strings;
            }
            }
            break;
        case DAFKA_PROTO_STORE_HELLO:
            content = zconfig_locate (config, "content");
            if (!content) {
                zsys_error ("Can't find 'content' section");
                dafka_proto_destroy (&self);
                return NULL;
            }
            {
            char *s = zconfig_get (content, "address", NULL);
            if (!s) {
                dafka_proto_destroy (&self);
                return NULL;
            }
            strncpy (self->address, s, 255);
            }
            break;
    }
    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the dafka_proto

void
dafka_proto_destroy (dafka_proto_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        dafka_proto_t *self = *self_p;

        //  Free class properties
        zstr_free (&self->topic);
        zframe_destroy (&self->routing_id);
        zmq_msg_close (&self->content);
        if (self->subjects)
            zlist_destroy (&self->subjects);

        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}


//  --------------------------------------------------------------------------
//  Create a deep copy of a dafka_proto instance

dafka_proto_t *
dafka_proto_dup (dafka_proto_t *other)
{
    assert (other);
    dafka_proto_t *copy = dafka_proto_new ();

    // Copy the routing and message id
    dafka_proto_set_routing_id (copy, dafka_proto_routing_id (other));
    dafka_proto_set_id (copy, dafka_proto_id (other));

    // Copy the topic
    dafka_proto_set_topic (copy, dafka_proto_topic (other));

    // Copy the rest of the fields
    dafka_proto_set_address (copy, dafka_proto_address (other));
    dafka_proto_set_subject (copy, dafka_proto_subject (other));
    dafka_proto_set_sequence (copy, dafka_proto_sequence (other));
    {
        zmq_msg_t dup_frame;
        zmq_msg_init (&dup_frame);
        zmq_msg_copy (&dup_frame, dafka_proto_content (other));
        dafka_proto_set_content (copy, &dup_frame);
    }
    dafka_proto_set_count (copy, dafka_proto_count (other));
    {
        zlist_t *lcopy = zlist_dup (dafka_proto_subjects (other));
        dafka_proto_set_subjects (copy, &lcopy);
    }

    return copy;
}


//  --------------------------------------------------------------------------
//  Receive a dafka_proto from the socket. Returns 0 if OK, -1 if
//  the recv was interrupted, or -2 if the message is malformed.
//  Blocks if there is no message waiting.

int
dafka_proto_recv (dafka_proto_t *self, zsock_t *input)
{
    assert (input);
    int rc = 0;
    zmq_msg_t frame;
    zmq_msg_init (&frame);

    if (zsock_type (input) == ZMQ_ROUTER) {
        zframe_destroy (&self->routing_id);
        self->routing_id = zframe_recv (input);
        if (!self->routing_id || !zsock_rcvmore (input)) {
            zsys_warning ("dafka_proto: no routing ID");
            rc = -1;            //  Interrupted
            goto malformed;
        }
    }
    int size;
    size = zmq_msg_recv (&frame, zsock_resolve (input), 0);
    if (size == -1) {
        if (errno != EAGAIN)
            zsys_warning ("dafka_proto: interrupted");
        rc = -1;                //  Interrupted
        goto malformed;
    }
    self->needle = (byte *) zmq_msg_data (&frame);
    self->ceiling = self->needle + zmq_msg_size (&frame);

    if (zsock_type (input) == ZMQ_XPUB) {
        byte is_subscribe;
        GET_NUMBER1 (is_subscribe);
        self->is_subscribe = is_subscribe == 1;
        GET_NUMBER1 (self->id);
        zstr_free (&self->topic);
        size_t topic_size = self->ceiling - self->needle;
        self->topic = (char *) malloc (topic_size + 1);
        memcpy (self->topic, self->needle, topic_size);
        self->topic[topic_size] = '\0';
        zmq_msg_close (&frame);
        return rc;
    }

    GET_NUMBER1 (self->id);
    zstr_free (&self->topic);
    size_t topic_size = strnlen ((char *) self->needle, self->ceiling - self->needle - 1);
    self->topic = (char *) malloc (topic_size + 1);
    memcpy (self->topic, self->needle, topic_size + 1);
    self->needle += topic_size + 1;

    switch (self->id) {
        case DAFKA_PROTO_RECORD:
            {
                byte version;
                GET_NUMBER1 (version);
                if (version != 1) {
                    zsys_warning ("dafka_proto: version is invalid");
                    rc = -2;    //  Malformed
                    goto malformed;
                }
            }
            GET_STRING (self->address);
            GET_STRING (self->subject);
            GET_NUMBER8 (self->sequence);
            //  Get next frame off socket
            if (!zsock_rcvmore (input)) {
                zsys_warning ("dafka_proto: content is missing");
                rc = -2;        //  Malformed
                goto malformed;
            }
            zmq_msg_recv (&self->content, zsock_resolve (input), 0);
            break;

        case DAFKA_PROTO_DIRECT_RECORD:
            {
                byte version;
                GET_NUMBER1 (version);
                if (version != 1) {
                    zsys_warning ("dafka_proto: version is invalid");
                    rc = -2;    //  Malformed
                    goto malformed;
                }
            }
            GET_STRING (self->address);
            GET_STRING (self->subject);
            GET_NUMBER8 (self->sequence);
            //  Get next frame off socket
            if (!zsock_rcvmore (input)) {
                zsys_warning ("dafka_proto: content is missing");
                rc = -2;        //  Malformed
                goto malformed;
            }
            zmq_msg_recv (&self->content, zsock_resolve (input), 0);
            break;

        case DAFKA_PROTO_FETCH:
            {
                byte version;
                GET_NUMBER1 (version);
                if (version != 1) {
                    zsys_warning ("dafka_proto: version is invalid");
                    rc = -2;    //  Malformed
                    goto malformed;
                }
            }
            GET_STRING (self->address);
            GET_STRING (self->subject);
            GET_NUMBER8 (self->sequence);
            GET_NUMBER4 (self->count);
            break;

        case DAFKA_PROTO_ACK:
            {
                byte version;
                GET_NUMBER1 (version);
                if (version != 1) {
                    zsys_warning ("dafka_proto: version is invalid");
                    rc = -2;    //  Malformed
                    goto malformed;
                }
            }
            GET_STRING (self->subject);
            GET_NUMBER8 (self->sequence);
            break;

        case DAFKA_PROTO_HEAD:
            {
                byte version;
                GET_NUMBER1 (version);
                if (version != 1) {
                    zsys_warning ("dafka_proto: version is invalid");
                    rc = -2;    //  Malformed
                    goto malformed;
                }
            }
            GET_STRING (self->address);
            GET_STRING (self->subject);
            GET_NUMBER8 (self->sequence);
            break;

        case DAFKA_PROTO_DIRECT_HEAD:
            {
                byte version;
                GET_NUMBER1 (version);
                if (version != 1) {
                    zsys_warning ("dafka_proto: version is invalid");
                    rc = -2;    //  Malformed
                    goto malformed;
                }
            }
            GET_STRING (self->address);
            GET_STRING (self->subject);
            GET_NUMBER8 (self->sequence);
            break;

        case DAFKA_PROTO_GET_HEADS:
            {
                byte version;
                GET_NUMBER1 (version);
                if (version != 1) {
                    zsys_warning ("dafka_proto: version is invalid");
                    rc = -2;    //  Malformed
                    goto malformed;
                }
            }
            GET_STRING (self->address);
            break;

        case DAFKA_PROTO_CONSUMER_HELLO:
            {
                byte version;
                GET_NUMBER1 (version);
                if (version != 1) {
                    zsys_warning ("dafka_proto: version is invalid");
                    rc = -2;    //  Malformed
                    goto malformed;
                }
            }
            GET_STRING (self->address);
            {
                size_t list_size;
                GET_NUMBER4 (list_size);
                zlist_destroy (&self->subjects);
                self->subjects = zlist_new ();
                zlist_autofree (self->subjects);
                while (list_size--) {
                    char *string = NULL;
                    GET_LONGSTR (string);
                    zlist_append (self->subjects, string);
                    free (string);
                }
            }
            break;

        case DAFKA_PROTO_STORE_HELLO:
            {
                byte version;
                GET_NUMBER1 (version);
                if (version != 1) {
                    zsys_warning ("dafka_proto: version is invalid");
                    rc = -2;    //  Malformed
                    goto malformed;
                }
            }
            GET_STRING (self->address);
            break;

        default:
            zsys_warning ("dafka_proto: bad message ID");
            rc = -2;            //  Malformed
            goto malformed;
    }
    //  Successful return
    zmq_msg_close (&frame);
    return rc;

    //  Error returns
    malformed:
        zmq_msg_close (&frame);
        return rc;              //  Invalid message
}


//  --------------------------------------------------------------------------
//  Send the dafka_proto to the socket. Does not destroy it. Returns 0 if
//  OK, else -1.

int
dafka_proto_send (dafka_proto_t *self, zsock_t *output)
{
    assert (self);
    assert (output);

    if (zsock_type (output) == ZMQ_ROUTER)
        zframe_send (&self->routing_id, output, ZFRAME_MORE + ZFRAME_REUSE);

    size_t frame_size = 1 + strlen(self->topic) + 1; //  Message ID, topic and NULL

    switch (self->id) {
        case DAFKA_PROTO_RECORD:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            frame_size += 1 + strlen (self->subject);
            frame_size += 8;            //  sequence
            break;
        case DAFKA_PROTO_DIRECT_RECORD:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            frame_size += 1 + strlen (self->subject);
            frame_size += 8;            //  sequence
            break;
        case DAFKA_PROTO_FETCH:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            frame_size += 1 + strlen (self->subject);
            frame_size += 8;            //  sequence
            frame_size += 4;            //  count
            break;
        case DAFKA_PROTO_ACK:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->subject);
            frame_size += 8;            //  sequence
            break;
        case DAFKA_PROTO_HEAD:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            frame_size += 1 + strlen (self->subject);
            frame_size += 8;            //  sequence
            break;
        case DAFKA_PROTO_DIRECT_HEAD:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            frame_size += 1 + strlen (self->subject);
            frame_size += 8;            //  sequence
            break;
        case DAFKA_PROTO_GET_HEADS:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            break;
        case DAFKA_PROTO_CONSUMER_HELLO:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            frame_size += 4;            //  Size is 4 octets
            if (self->subjects) {
                char *subjects = (char *) zlist_first (self->subjects);
                while (subjects) {
                    frame_size += 4 + strlen (subjects);
                    subjects = (char *) zlist_next (self->subjects);
                }
            }
            break;
        case DAFKA_PROTO_STORE_HELLO:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            break;
    }

    zmq_msg_t frame;
    zmq_msg_init_size (&frame, frame_size);
    self->needle = (byte *) zmq_msg_data (&frame);

    //  Now serialize message into the frame
    PUT_NUMBER1 (self->id);
    size_t topic_size = strlen (self->topic);
    memcpy(self->needle, self->topic, topic_size + 1);
    self->needle += topic_size + 1;
    size_t nbr_frames = 1;              //  Total number of frames to send

    switch (self->id) {
        case DAFKA_PROTO_RECORD:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            PUT_STRING (self->subject);
            PUT_NUMBER8 (self->sequence);
            nbr_frames++;
            break;

        case DAFKA_PROTO_DIRECT_RECORD:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            PUT_STRING (self->subject);
            PUT_NUMBER8 (self->sequence);
            nbr_frames++;
            break;

        case DAFKA_PROTO_FETCH:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            PUT_STRING (self->subject);
            PUT_NUMBER8 (self->sequence);
            PUT_NUMBER4 (self->count);
            break;

        case DAFKA_PROTO_ACK:
            PUT_NUMBER1 (1);
            PUT_STRING (self->subject);
            PUT_NUMBER8 (self->sequence);
            break;

        case DAFKA_PROTO_HEAD:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            PUT_STRING (self->subject);
            PUT_NUMBER8 (self->sequence);
            break;

        case DAFKA_PROTO_DIRECT_HEAD:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            PUT_STRING (self->subject);
            PUT_NUMBER8 (self->sequence);
            break;

        case DAFKA_PROTO_GET_HEADS:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            break;

        case DAFKA_PROTO_CONSUMER_HELLO:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            if (self->subjects) {
                PUT_NUMBER4 (zlist_size (self->subjects));
                char *subjects = (char *) zlist_first (self->subjects);
                while (subjects) {
                    PUT_LONGSTR (subjects);
                    subjects = (char *) zlist_next (self->subjects);
                }
            }
            else
                PUT_NUMBER4 (0);    //  Empty string array
            break;

        case DAFKA_PROTO_STORE_HELLO:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            break;

    }

    //  Now send the data frame
    zmq_msg_send (&frame, zsock_resolve (output), --nbr_frames? ZMQ_SNDMORE: 0);

    //  Now send any frame fields, in order
    if (self->id == DAFKA_PROTO_RECORD) {
        zmq_msg_t copy;
        zmq_msg_init (&copy);
        zmq_msg_copy (&copy, &self->content);
        zmq_msg_send (&copy, zsock_resolve (output), (--nbr_frames? ZMQ_SNDMORE: 0));
        // Optimize by not calling zmq_msg_close, as the copy is an empty one
    }
    //  Now send any frame fields, in order
    if (self->id == DAFKA_PROTO_DIRECT_RECORD) {
        zmq_msg_t copy;
        zmq_msg_init (&copy);
        zmq_msg_copy (&copy, &self->content);
        zmq_msg_send (&copy, zsock_resolve (output), (--nbr_frames? ZMQ_SNDMORE: 0));
        // Optimize by not calling zmq_msg_close, as the copy is an empty one
    }
    return 0;
}

//  --------------------------------------------------------------------------
//  Encode the first frame of dafka_proto. Does not destroy it. Returns the frame if
//  OK, else NULL.

zframe_t *
dafka_proto_encode (dafka_proto_t *self)
{
    assert (self);

    size_t frame_size = 1 + strlen(self->topic) + 1; //  Message ID, topic and NULL

    switch (self->id) {
        case DAFKA_PROTO_RECORD:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            frame_size += 1 + strlen (self->subject);
            frame_size += 8;            //  sequence
            break;
        case DAFKA_PROTO_DIRECT_RECORD:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            frame_size += 1 + strlen (self->subject);
            frame_size += 8;            //  sequence
            break;
        case DAFKA_PROTO_FETCH:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            frame_size += 1 + strlen (self->subject);
            frame_size += 8;            //  sequence
            frame_size += 4;            //  count
            break;
        case DAFKA_PROTO_ACK:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->subject);
            frame_size += 8;            //  sequence
            break;
        case DAFKA_PROTO_HEAD:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            frame_size += 1 + strlen (self->subject);
            frame_size += 8;            //  sequence
            break;
        case DAFKA_PROTO_DIRECT_HEAD:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            frame_size += 1 + strlen (self->subject);
            frame_size += 8;            //  sequence
            break;
        case DAFKA_PROTO_GET_HEADS:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            break;
        case DAFKA_PROTO_CONSUMER_HELLO:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            frame_size += 4;            //  Size is 4 octets
            if (self->subjects) {
                char *subjects = (char *) zlist_first (self->subjects);
                while (subjects) {
                    frame_size += 4 + strlen (subjects);
                    subjects = (char *) zlist_next (self->subjects);
                }
            }
            break;
        case DAFKA_PROTO_STORE_HELLO:
            frame_size += 1;            //  version
            frame_size += 1 + strlen (self->address);
            break;
    }

    zframe_t *frame = zframe_new (NULL, frame_size);
    self->needle = (byte *) zframe_data (frame);

    //  Now serialize message into the frame
    PUT_NUMBER1 (self->id);
    size_t topic_size = strlen (self->topic);
    memcpy(self->needle, self->topic, topic_size + 1);
    self->needle += topic_size + 1;
    size_t nbr_frames = 1;              //  Total number of frames to send

    switch (self->id) {
        case DAFKA_PROTO_RECORD:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            PUT_STRING (self->subject);
            PUT_NUMBER8 (self->sequence);
            nbr_frames++;
            break;

        case DAFKA_PROTO_DIRECT_RECORD:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            PUT_STRING (self->subject);
            PUT_NUMBER8 (self->sequence);
            nbr_frames++;
            break;

        case DAFKA_PROTO_FETCH:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            PUT_STRING (self->subject);
            PUT_NUMBER8 (self->sequence);
            PUT_NUMBER4 (self->count);
            break;

        case DAFKA_PROTO_ACK:
            PUT_NUMBER1 (1);
            PUT_STRING (self->subject);
            PUT_NUMBER8 (self->sequence);
            break;

        case DAFKA_PROTO_HEAD:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            PUT_STRING (self->subject);
            PUT_NUMBER8 (self->sequence);
            break;

        case DAFKA_PROTO_DIRECT_HEAD:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            PUT_STRING (self->subject);
            PUT_NUMBER8 (self->sequence);
            break;

        case DAFKA_PROTO_GET_HEADS:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            break;

        case DAFKA_PROTO_CONSUMER_HELLO:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            if (self->subjects) {
                PUT_NUMBER4 (zlist_size (self->subjects));
                char *subjects = (char *) zlist_first (self->subjects);
                while (subjects) {
                    PUT_LONGSTR (subjects);
                    subjects = (char *) zlist_next (self->subjects);
                }
            }
            else
                PUT_NUMBER4 (0);    //  Empty string array
            break;

        case DAFKA_PROTO_STORE_HELLO:
            PUT_NUMBER1 (1);
            PUT_STRING (self->address);
            break;

    }

    return frame;
}


//  --------------------------------------------------------------------------
//  Print contents of message to stdout

void
dafka_proto_print (dafka_proto_t *self)
{
    assert (self);
    switch (self->id) {
        case DAFKA_PROTO_RECORD:
            zsys_debug ("DAFKA_PROTO_RECORD:");
            zsys_debug ("    topic='%s'", self->topic);
            zsys_debug ("    version=1");
            zsys_debug ("    address='%s'", self->address);
            zsys_debug ("    subject='%s'", self->subject);
            zsys_debug ("    sequence=%ld", (long) self->sequence);
            zsys_debug ("    content=");
            {
                zframe_t *frame = zframe_new (zmq_msg_data (&self->content), zmq_msg_size (&self->content));
                zframe_print (frame, NULL);
                zframe_destroy (&frame);
            }
            break;

        case DAFKA_PROTO_DIRECT_RECORD:
            zsys_debug ("DAFKA_PROTO_DIRECT_RECORD:");
            zsys_debug ("    topic='%s'", self->topic);
            zsys_debug ("    version=1");
            zsys_debug ("    address='%s'", self->address);
            zsys_debug ("    subject='%s'", self->subject);
            zsys_debug ("    sequence=%ld", (long) self->sequence);
            zsys_debug ("    content=");
            {
                zframe_t *frame = zframe_new (zmq_msg_data (&self->content), zmq_msg_size (&self->content));
                zframe_print (frame, NULL);
                zframe_destroy (&frame);
            }
            break;

        case DAFKA_PROTO_FETCH:
            zsys_debug ("DAFKA_PROTO_FETCH:");
            zsys_debug ("    topic='%s'", self->topic);
            zsys_debug ("    version=1");
            zsys_debug ("    address='%s'", self->address);
            zsys_debug ("    subject='%s'", self->subject);
            zsys_debug ("    sequence=%ld", (long) self->sequence);
            zsys_debug ("    count=%ld", (long) self->count);
            break;

        case DAFKA_PROTO_ACK:
            zsys_debug ("DAFKA_PROTO_ACK:");
            zsys_debug ("    topic='%s'", self->topic);
            zsys_debug ("    version=1");
            zsys_debug ("    subject='%s'", self->subject);
            zsys_debug ("    sequence=%ld", (long) self->sequence);
            break;

        case DAFKA_PROTO_HEAD:
            zsys_debug ("DAFKA_PROTO_HEAD:");
            zsys_debug ("    topic='%s'", self->topic);
            zsys_debug ("    version=1");
            zsys_debug ("    address='%s'", self->address);
            zsys_debug ("    subject='%s'", self->subject);
            zsys_debug ("    sequence=%ld", (long) self->sequence);
            break;

        case DAFKA_PROTO_DIRECT_HEAD:
            zsys_debug ("DAFKA_PROTO_DIRECT_HEAD:");
            zsys_debug ("    topic='%s'", self->topic);
            zsys_debug ("    version=1");
            zsys_debug ("    address='%s'", self->address);
            zsys_debug ("    subject='%s'", self->subject);
            zsys_debug ("    sequence=%ld", (long) self->sequence);
            break;

        case DAFKA_PROTO_GET_HEADS:
            zsys_debug ("DAFKA_PROTO_GET_HEADS:");
            zsys_debug ("    topic='%s'", self->topic);
            zsys_debug ("    version=1");
            zsys_debug ("    address='%s'", self->address);
            break;

        case DAFKA_PROTO_CONSUMER_HELLO:
            zsys_debug ("DAFKA_PROTO_CONSUMER_HELLO:");
            zsys_debug ("    topic='%s'", self->topic);
            zsys_debug ("    version=1");
            zsys_debug ("    address='%s'", self->address);
            zsys_debug ("    subjects=");
            if (self->subjects) {
                char *subjects = (char *) zlist_first (self->subjects);
                while (subjects) {
                    zsys_debug ("        '%s'", subjects);
                    subjects = (char *) zlist_next (self->subjects);
                }
            }
            break;

        case DAFKA_PROTO_STORE_HELLO:
            zsys_debug ("DAFKA_PROTO_STORE_HELLO:");
            zsys_debug ("    topic='%s'", self->topic);
            zsys_debug ("    version=1");
            zsys_debug ("    address='%s'", self->address);
            break;

    }
}

//  --------------------------------------------------------------------------
//  Export class as zconfig_t*. Caller is responsibe for destroying the instance

zconfig_t *
dafka_proto_zpl (dafka_proto_t *self, zconfig_t *parent)
{
    assert (self);

    zconfig_t *root = zconfig_new ("dafka_proto", parent);

    switch (self->id) {
        case DAFKA_PROTO_RECORD:
        {
            zconfig_put (root, "message", "DAFKA_PROTO_RECORD");

            if (self->routing_id) {
                char *hex = NULL;
                STR_FROM_BYTES (hex, zframe_data (self->routing_id), zframe_size (self->routing_id));
                zconfig_putf (root, "routing_id", "%s", hex);
                zstr_free (&hex);
            }

            zconfig_putf (root, "topic", "%s", self->topic);

            zconfig_t *config = zconfig_new ("content", root);
            zconfig_putf (config, "version", "%s", "1");
            zconfig_putf (config, "address", "%s", self->address);
            zconfig_putf (config, "subject", "%s", self->subject);
            zconfig_putf (config, "sequence", "%ld", (long) self->sequence);
            {
            char *hex = NULL;
            STR_FROM_BYTES (hex, zmq_msg_data (&self->content), zmq_msg_size (&self->content));
            zconfig_putf (config, "content", "%s", hex);
            zstr_free (&hex);
            }
            break;
            }
        case DAFKA_PROTO_DIRECT_RECORD:
        {
            zconfig_put (root, "message", "DAFKA_PROTO_DIRECT_RECORD");

            if (self->routing_id) {
                char *hex = NULL;
                STR_FROM_BYTES (hex, zframe_data (self->routing_id), zframe_size (self->routing_id));
                zconfig_putf (root, "routing_id", "%s", hex);
                zstr_free (&hex);
            }

            zconfig_putf (root, "topic", "%s", self->topic);

            zconfig_t *config = zconfig_new ("content", root);
            zconfig_putf (config, "version", "%s", "1");
            zconfig_putf (config, "address", "%s", self->address);
            zconfig_putf (config, "subject", "%s", self->subject);
            zconfig_putf (config, "sequence", "%ld", (long) self->sequence);
            {
            char *hex = NULL;
            STR_FROM_BYTES (hex, zmq_msg_data (&self->content), zmq_msg_size (&self->content));
            zconfig_putf (config, "content", "%s", hex);
            zstr_free (&hex);
            }
            break;
            }
        case DAFKA_PROTO_FETCH:
        {
            zconfig_put (root, "message", "DAFKA_PROTO_FETCH");

            if (self->routing_id) {
                char *hex = NULL;
                STR_FROM_BYTES (hex, zframe_data (self->routing_id), zframe_size (self->routing_id));
                zconfig_putf (root, "routing_id", "%s", hex);
                zstr_free (&hex);
            }

            zconfig_putf (root, "topic", "%s", self->topic);

            zconfig_t *config = zconfig_new ("content", root);
            zconfig_putf (config, "version", "%s", "1");
            zconfig_putf (config, "address", "%s", self->address);
            zconfig_putf (config, "subject", "%s", self->subject);
            zconfig_putf (config, "sequence", "%ld", (long) self->sequence);
            zconfig_putf (config, "count", "%ld", (long) self->count);
            break;
            }
        case DAFKA_PROTO_ACK:
        {
            zconfig_put (root, "message", "DAFKA_PROTO_ACK");

            if (self->routing_id) {
                char *hex = NULL;
                STR_FROM_BYTES (hex, zframe_data (self->routing_id), zframe_size (self->routing_id));
                zconfig_putf (root, "routing_id", "%s", hex);
                zstr_free (&hex);
            }

            zconfig_putf (root, "topic", "%s", self->topic);

            zconfig_t *config = zconfig_new ("content", root);
            zconfig_putf (config, "version", "%s", "1");
            zconfig_putf (config, "subject", "%s", self->subject);
            zconfig_putf (config, "sequence", "%ld", (long) self->sequence);
            break;
            }
        case DAFKA_PROTO_HEAD:
        {
            zconfig_put (root, "message", "DAFKA_PROTO_HEAD");

            if (self->routing_id) {
                char *hex = NULL;
                STR_FROM_BYTES (hex, zframe_data (self->routing_id), zframe_size (self->routing_id));
                zconfig_putf (root, "routing_id", "%s", hex);
                zstr_free (&hex);
            }

            zconfig_putf (root, "topic", "%s", self->topic);

            zconfig_t *config = zconfig_new ("content", root);
            zconfig_putf (config, "version", "%s", "1");
            zconfig_putf (config, "address", "%s", self->address);
            zconfig_putf (config, "subject", "%s", self->subject);
            zconfig_putf (config, "sequence", "%ld", (long) self->sequence);
            break;
            }
        case DAFKA_PROTO_DIRECT_HEAD:
        {
            zconfig_put (root, "message", "DAFKA_PROTO_DIRECT_HEAD");

            if (self->routing_id) {
                char *hex = NULL;
                STR_FROM_BYTES (hex, zframe_data (self->routing_id), zframe_size (self->routing_id));
                zconfig_putf (root, "routing_id", "%s", hex);
                zstr_free (&hex);
            }

            zconfig_putf (root, "topic", "%s", self->topic);

            zconfig_t *config = zconfig_new ("content", root);
            zconfig_putf (config, "version", "%s", "1");
            zconfig_putf (config, "address", "%s", self->address);
            zconfig_putf (config, "subject", "%s", self->subject);
            zconfig_putf (config, "sequence", "%ld", (long) self->sequence);
            break;
            }
        case DAFKA_PROTO_GET_HEADS:
        {
            zconfig_put (root, "message", "DAFKA_PROTO_GET_HEADS");

            if (self->routing_id) {
                char *hex = NULL;
                STR_FROM_BYTES (hex, zframe_data (self->routing_id), zframe_size (self->routing_id));
                zconfig_putf (root, "routing_id", "%s", hex);
                zstr_free (&hex);
            }

            zconfig_putf (root, "topic", "%s", self->topic);

            zconfig_t *config = zconfig_new ("content", root);
            zconfig_putf (config, "version", "%s", "1");
            zconfig_putf (config, "address", "%s", self->address);
            break;
            }
        case DAFKA_PROTO_CONSUMER_HELLO:
        {
            zconfig_put (root, "message", "DAFKA_PROTO_CONSUMER_HELLO");

            if (self->routing_id) {
                char *hex = NULL;
                STR_FROM_BYTES (hex, zframe_data (self->routing_id), zframe_size (self->routing_id));
                zconfig_putf (root, "routing_id", "%s", hex);
                zstr_free (&hex);
            }

            zconfig_putf (root, "topic", "%s", self->topic);

            zconfig_t *config = zconfig_new ("content", root);
            zconfig_putf (config, "version", "%s", "1");
            zconfig_putf (config, "address", "%s", self->address);
            if (self->subjects) {
                zconfig_t *strings = zconfig_new ("subjects", config);
                size_t i = 0;
                char *subjects = (char *) zlist_first (self->subjects);
                while (subjects) {
                    char *key = zsys_sprintf ("%zu", i);
                    zconfig_putf (strings, key, "%s", subjects);
                    zstr_free (&key);
                    i++;
                    subjects = (char *) zlist_next (self->subjects);
                }
            }
            break;
            }
        case DAFKA_PROTO_STORE_HELLO:
        {
            zconfig_put (root, "message", "DAFKA_PROTO_STORE_HELLO");

            if (self->routing_id) {
                char *hex = NULL;
                STR_FROM_BYTES (hex, zframe_data (self->routing_id), zframe_size (self->routing_id));
                zconfig_putf (root, "routing_id", "%s", hex);
                zstr_free (&hex);
            }

            zconfig_putf (root, "topic", "%s", self->topic);

            zconfig_t *config = zconfig_new ("content", root);
            zconfig_putf (config, "version", "%s", "1");
            zconfig_putf (config, "address", "%s", self->address);
            break;
            }
    }
    return root;
}

//  --------------------------------------------------------------------------
//  Get/set the message routing_id

zframe_t *
dafka_proto_routing_id (dafka_proto_t *self)
{
    assert (self);
    return self->routing_id;
}

void
dafka_proto_set_routing_id (dafka_proto_t *self, zframe_t *routing_id)
{
    if (self->routing_id)
        zframe_destroy (&self->routing_id);
    self->routing_id = zframe_dup (routing_id);
}


//  --------------------------------------------------------------------------
//  Get/set the dafka_proto id

char
dafka_proto_id (dafka_proto_t *self)
{
    assert (self);
    return self->id;
}

void
dafka_proto_set_id (dafka_proto_t *self, char id)
{
    self->id = id;
}

//  --------------------------------------------------------------------------
//  Return a printable command string

const char *
dafka_proto_command (dafka_proto_t *self)
{
    assert (self);
    switch (self->id) {
        case DAFKA_PROTO_RECORD:
            return ("RECORD");
            break;
        case DAFKA_PROTO_DIRECT_RECORD:
            return ("DIRECT_RECORD");
            break;
        case DAFKA_PROTO_FETCH:
            return ("FETCH");
            break;
        case DAFKA_PROTO_ACK:
            return ("ACK");
            break;
        case DAFKA_PROTO_HEAD:
            return ("HEAD");
            break;
        case DAFKA_PROTO_DIRECT_HEAD:
            return ("DIRECT_HEAD");
            break;
        case DAFKA_PROTO_GET_HEADS:
            return ("GET_HEADS");
            break;
        case DAFKA_PROTO_CONSUMER_HELLO:
            return ("CONSUMER_HELLO");
            break;
        case DAFKA_PROTO_STORE_HELLO:
            return ("STORE_HELLO");
            break;
    }
    return "?";
}

//  --------------------------------------------------------------------------
//  Get/set the topic field

const char *
dafka_proto_topic (dafka_proto_t *self)
{
    assert (self);
    return self->topic;
}

void
dafka_proto_set_topic (dafka_proto_t *self, const char *topic)
{
    assert (self);
    zstr_free (&self->topic);
    self->topic = strdup (topic);
}

//  --------------------------------------------------------------------------
//  Subscribe/Unsubscribe for SUB socket

void
dafka_proto_subscribe (zsock_t *sub, char id, const char *topic) {
    char* subscription = zsys_sprintf ("%c%s",id, topic);
    zsock_set_subscribe (sub, subscription);
    zstr_free (&subscription);
}

void
dafka_proto_unsubscribe (zsock_t *sub, char id, const char *topic) {
    char* subscription = zsys_sprintf ("%c%s", id, topic);
    zsock_set_unsubscribe (sub, subscription);
    zstr_free (&subscription);
}

//  --------------------------------------------------------------------------
//  Get the type of subscription received from a XPUB socket

bool
dafka_proto_is_subscribe (dafka_proto_t *self) {
    assert (self);
    return self->is_subscribe;
}

//  --------------------------------------------------------------------------
//  Get/set the address field

const char *
dafka_proto_address (dafka_proto_t *self)
{
    assert (self);
    return self->address;
}

void
dafka_proto_set_address (dafka_proto_t *self, const char *value)
{
    assert (self);
    assert (value);
    if (value == self->address)
        return;
    strncpy (self->address, value, 255);
    self->address [255] = 0;
}


//  --------------------------------------------------------------------------
//  Get/set the subject field

const char *
dafka_proto_subject (dafka_proto_t *self)
{
    assert (self);
    return self->subject;
}

void
dafka_proto_set_subject (dafka_proto_t *self, const char *value)
{
    assert (self);
    assert (value);
    if (value == self->subject)
        return;
    strncpy (self->subject, value, 255);
    self->subject [255] = 0;
}


//  --------------------------------------------------------------------------
//  Get/set the sequence field

uint64_t
dafka_proto_sequence (dafka_proto_t *self)
{
    assert (self);
    return self->sequence;
}

void
dafka_proto_set_sequence (dafka_proto_t *self, uint64_t sequence)
{
    assert (self);
    self->sequence = sequence;
}


//  --------------------------------------------------------------------------
//  Get the content field without transferring ownership

zmq_msg_t *
dafka_proto_content (dafka_proto_t *self)
{
    assert (self);
    return &self->content;
}

//  Get the content field and transfer ownership to caller

void
dafka_proto_get_content (dafka_proto_t *self, zmq_msg_t *msg)
{
    assert (self);

    int rc = zmq_msg_move (msg, &self->content);
    assert (rc == 0);
}

//  Set the content field, transferring ownership from caller

void
dafka_proto_set_content (dafka_proto_t *self, zmq_msg_t *msg)
{
    assert (self);

    int rc = zmq_msg_move (&self->content, msg);
    assert (rc == 0);
}


//  --------------------------------------------------------------------------
//  Get/set the count field

uint32_t
dafka_proto_count (dafka_proto_t *self)
{
    assert (self);
    return self->count;
}

void
dafka_proto_set_count (dafka_proto_t *self, uint32_t count)
{
    assert (self);
    self->count = count;
}


//  --------------------------------------------------------------------------
//  Get the subjects field, without transferring ownership

zlist_t *
dafka_proto_subjects (dafka_proto_t *self)
{
    assert (self);
    return self->subjects;
}

//  Get the subjects field and transfer ownership to caller

zlist_t *
dafka_proto_get_subjects (dafka_proto_t *self)
{
    assert (self);
    zlist_t *subjects = self->subjects;
    self->subjects = NULL;
    return subjects;
}

//  Set the subjects field, transferring ownership from caller

void
dafka_proto_set_subjects (dafka_proto_t *self, zlist_t **subjects_p)
{
    assert (self);
    assert (subjects_p);
    zlist_destroy (&self->subjects);
    self->subjects = *subjects_p;
    *subjects_p = NULL;
}




//  --------------------------------------------------------------------------
//  Selftest

void
dafka_proto_test (bool verbose)
{
    printf (" * dafka_proto: ");

    if (verbose)
        printf ("\n");

    //  @selftest
    //  Simple create/destroy test
    zconfig_t *config;
    dafka_proto_t *self = dafka_proto_new ();
    assert (self);
    dafka_proto_destroy (&self);
	static const int MAX_INSTANCE = 3;
    //  Create pair of sockets we can send through
    //  We must bind before connect if we wish to remain compatible with ZeroMQ < v4
    zsock_t *output = zsock_new (ZMQ_DEALER);
    assert (output);
    int rc = zsock_bind (output, "inproc://selftest-dafka_proto");
    assert (rc == 0);

    zsock_t *input = zsock_new (ZMQ_ROUTER);
    assert (input);
    rc = zsock_connect (input, "inproc://selftest-dafka_proto");
    assert (rc == 0);


    //  Encode/send/decode and verify each message type
    int instance;
    self = dafka_proto_new ();
    dafka_proto_set_id (self, DAFKA_PROTO_RECORD);
    dafka_proto_set_topic (self, "Hello");
    dafka_proto_set_address (self, "Life is short but Now lasts for ever");
    dafka_proto_set_subject (self, "Life is short but Now lasts for ever");
    dafka_proto_set_sequence (self, 123);
    zmq_msg_t record_content;
    zmq_msg_init_data (&record_content, "Captcha Diem", 12, NULL, NULL);
    dafka_proto_set_content (self, &record_content);
    zmq_msg_close (&record_content);
    // convert to zpl
    config = dafka_proto_zpl (self, NULL);
    if (verbose)
        zconfig_print (config);

    //  Send twice
    dafka_proto_send (self, output);
    dafka_proto_send (self, output);

    for (instance = 0; instance < MAX_INSTANCE; instance++) {
        dafka_proto_t *self_temp = self;
        if (instance < MAX_INSTANCE - 1)
            dafka_proto_recv (self, input);
        else {
            self = dafka_proto_new_zpl (config);
            assert (self);
            zconfig_destroy (&config);
        }
        if (instance < MAX_INSTANCE - 1)
            assert (dafka_proto_routing_id (self));
        assert (streq (dafka_proto_topic (self), "Hello"));
        assert (streq (dafka_proto_address (self), "Life is short but Now lasts for ever"));
        assert (streq (dafka_proto_subject (self), "Life is short but Now lasts for ever"));
        assert (dafka_proto_sequence (self) == 123);
        assert (memcmp (zmq_msg_data (dafka_proto_content (self)), "Captcha Diem", 12) == 0);
        if (instance == MAX_INSTANCE - 1) {
            dafka_proto_destroy (&self);
            self = self_temp;
        }
    }
    dafka_proto_set_id (self, DAFKA_PROTO_DIRECT_RECORD);
    dafka_proto_set_topic (self, "Hello");
    dafka_proto_set_address (self, "Life is short but Now lasts for ever");
    dafka_proto_set_subject (self, "Life is short but Now lasts for ever");
    dafka_proto_set_sequence (self, 123);
    zmq_msg_t direct_record_content;
    zmq_msg_init_data (&direct_record_content, "Captcha Diem", 12, NULL, NULL);
    dafka_proto_set_content (self, &direct_record_content);
    zmq_msg_close (&direct_record_content);
    // convert to zpl
    config = dafka_proto_zpl (self, NULL);
    if (verbose)
        zconfig_print (config);

    //  Send twice
    dafka_proto_send (self, output);
    dafka_proto_send (self, output);

    for (instance = 0; instance < MAX_INSTANCE; instance++) {
        dafka_proto_t *self_temp = self;
        if (instance < MAX_INSTANCE - 1)
            dafka_proto_recv (self, input);
        else {
            self = dafka_proto_new_zpl (config);
            assert (self);
            zconfig_destroy (&config);
        }
        if (instance < MAX_INSTANCE - 1)
            assert (dafka_proto_routing_id (self));
        assert (streq (dafka_proto_topic (self), "Hello"));
        assert (streq (dafka_proto_address (self), "Life is short but Now lasts for ever"));
        assert (streq (dafka_proto_subject (self), "Life is short but Now lasts for ever"));
        assert (dafka_proto_sequence (self) == 123);
        assert (memcmp (zmq_msg_data (dafka_proto_content (self)), "Captcha Diem", 12) == 0);
        if (instance == MAX_INSTANCE - 1) {
            dafka_proto_destroy (&self);
            self = self_temp;
        }
    }
    dafka_proto_set_id (self, DAFKA_PROTO_FETCH);
    dafka_proto_set_topic (self, "Hello");
    dafka_proto_set_address (self, "Life is short but Now lasts for ever");
    dafka_proto_set_subject (self, "Life is short but Now lasts for ever");
    dafka_proto_set_sequence (self, 123);
    dafka_proto_set_count (self, 123);
    // convert to zpl
    config = dafka_proto_zpl (self, NULL);
    if (verbose)
        zconfig_print (config);

    //  Send twice
    dafka_proto_send (self, output);
    dafka_proto_send (self, output);

    for (instance = 0; instance < MAX_INSTANCE; instance++) {
        dafka_proto_t *self_temp = self;
        if (instance < MAX_INSTANCE - 1)
            dafka_proto_recv (self, input);
        else {
            self = dafka_proto_new_zpl (config);
            assert (self);
            zconfig_destroy (&config);
        }
        if (instance < MAX_INSTANCE - 1)
            assert (dafka_proto_routing_id (self));
        assert (streq (dafka_proto_topic (self), "Hello"));
        assert (streq (dafka_proto_address (self), "Life is short but Now lasts for ever"));
        assert (streq (dafka_proto_subject (self), "Life is short but Now lasts for ever"));
        assert (dafka_proto_sequence (self) == 123);
        assert (dafka_proto_count (self) == 123);
        if (instance == MAX_INSTANCE - 1) {
            dafka_proto_destroy (&self);
            self = self_temp;
        }
    }
    dafka_proto_set_id (self, DAFKA_PROTO_ACK);
    dafka_proto_set_topic (self, "Hello");
    dafka_proto_set_subject (self, "Life is short but Now lasts for ever");
    dafka_proto_set_sequence (self, 123);
    // convert to zpl
    config = dafka_proto_zpl (self, NULL);
    if (verbose)
        zconfig_print (config);

    //  Send twice
    dafka_proto_send (self, output);
    dafka_proto_send (self, output);

    for (instance = 0; instance < MAX_INSTANCE; instance++) {
        dafka_proto_t *self_temp = self;
        if (instance < MAX_INSTANCE - 1)
            dafka_proto_recv (self, input);
        else {
            self = dafka_proto_new_zpl (config);
            assert (self);
            zconfig_destroy (&config);
        }
        if (instance < MAX_INSTANCE - 1)
            assert (dafka_proto_routing_id (self));
        assert (streq (dafka_proto_topic (self), "Hello"));
        assert (streq (dafka_proto_subject (self), "Life is short but Now lasts for ever"));
        assert (dafka_proto_sequence (self) == 123);
        if (instance == MAX_INSTANCE - 1) {
            dafka_proto_destroy (&self);
            self = self_temp;
        }
    }
    dafka_proto_set_id (self, DAFKA_PROTO_HEAD);
    dafka_proto_set_topic (self, "Hello");
    dafka_proto_set_address (self, "Life is short but Now lasts for ever");
    dafka_proto_set_subject (self, "Life is short but Now lasts for ever");
    dafka_proto_set_sequence (self, 123);
    // convert to zpl
    config = dafka_proto_zpl (self, NULL);
    if (verbose)
        zconfig_print (config);

    //  Send twice
    dafka_proto_send (self, output);
    dafka_proto_send (self, output);

    for (instance = 0; instance < MAX_INSTANCE; instance++) {
        dafka_proto_t *self_temp = self;
        if (instance < MAX_INSTANCE - 1)
            dafka_proto_recv (self, input);
        else {
            self = dafka_proto_new_zpl (config);
            assert (self);
            zconfig_destroy (&config);
        }
        if (instance < MAX_INSTANCE - 1)
            assert (dafka_proto_routing_id (self));
        assert (streq (dafka_proto_topic (self), "Hello"));
        assert (streq (dafka_proto_address (self), "Life is short but Now lasts for ever"));
        assert (streq (dafka_proto_subject (self), "Life is short but Now lasts for ever"));
        assert (dafka_proto_sequence (self) == 123);
        if (instance == MAX_INSTANCE - 1) {
            dafka_proto_destroy (&self);
            self = self_temp;
        }
    }
    dafka_proto_set_id (self, DAFKA_PROTO_DIRECT_HEAD);
    dafka_proto_set_topic (self, "Hello");
    dafka_proto_set_address (self, "Life is short but Now lasts for ever");
    dafka_proto_set_subject (self, "Life is short but Now lasts for ever");
    dafka_proto_set_sequence (self, 123);
    // convert to zpl
    config = dafka_proto_zpl (self, NULL);
    if (verbose)
        zconfig_print (config);

    //  Send twice
    dafka_proto_send (self, output);
    dafka_proto_send (self, output);

    for (instance = 0; instance < MAX_INSTANCE; instance++) {
        dafka_proto_t *self_temp = self;
        if (instance < MAX_INSTANCE - 1)
            dafka_proto_recv (self, input);
        else {
            self = dafka_proto_new_zpl (config);
            assert (self);
            zconfig_destroy (&config);
        }
        if (instance < MAX_INSTANCE - 1)
            assert (dafka_proto_routing_id (self));
        assert (streq (dafka_proto_topic (self), "Hello"));
        assert (streq (dafka_proto_address (self), "Life is short but Now lasts for ever"));
        assert (streq (dafka_proto_subject (self), "Life is short but Now lasts for ever"));
        assert (dafka_proto_sequence (self) == 123);
        if (instance == MAX_INSTANCE - 1) {
            dafka_proto_destroy (&self);
            self = self_temp;
        }
    }
    dafka_proto_set_id (self, DAFKA_PROTO_GET_HEADS);
    dafka_proto_set_topic (self, "Hello");
    dafka_proto_set_address (self, "Life is short but Now lasts for ever");
    // convert to zpl
    config = dafka_proto_zpl (self, NULL);
    if (verbose)
        zconfig_print (config);

    //  Send twice
    dafka_proto_send (self, output);
    dafka_proto_send (self, output);

    for (instance = 0; instance < MAX_INSTANCE; instance++) {
        dafka_proto_t *self_temp = self;
        if (instance < MAX_INSTANCE - 1)
            dafka_proto_recv (self, input);
        else {
            self = dafka_proto_new_zpl (config);
            assert (self);
            zconfig_destroy (&config);
        }
        if (instance < MAX_INSTANCE - 1)
            assert (dafka_proto_routing_id (self));
        assert (streq (dafka_proto_topic (self), "Hello"));
        assert (streq (dafka_proto_address (self), "Life is short but Now lasts for ever"));
        if (instance == MAX_INSTANCE - 1) {
            dafka_proto_destroy (&self);
            self = self_temp;
        }
    }
    dafka_proto_set_id (self, DAFKA_PROTO_CONSUMER_HELLO);
    dafka_proto_set_topic (self, "Hello");
    dafka_proto_set_address (self, "Life is short but Now lasts for ever");
    zlist_t *consumer_hello_subjects = zlist_new ();
    zlist_append (consumer_hello_subjects, "Name: Brutus");
    zlist_append (consumer_hello_subjects, "Age: 43");
    dafka_proto_set_subjects (self, &consumer_hello_subjects);
    // convert to zpl
    config = dafka_proto_zpl (self, NULL);
    if (verbose)
        zconfig_print (config);

    //  Send twice
    dafka_proto_send (self, output);
    dafka_proto_send (self, output);

    for (instance = 0; instance < MAX_INSTANCE; instance++) {
        dafka_proto_t *self_temp = self;
        if (instance < MAX_INSTANCE - 1)
            dafka_proto_recv (self, input);
        else {
            self = dafka_proto_new_zpl (config);
            assert (self);
            zconfig_destroy (&config);
        }
        if (instance < MAX_INSTANCE - 1)
            assert (dafka_proto_routing_id (self));
        assert (streq (dafka_proto_topic (self), "Hello"));
        assert (streq (dafka_proto_address (self), "Life is short but Now lasts for ever"));
        zlist_t *subjects = dafka_proto_get_subjects (self);
        assert (subjects);
        assert (zlist_size (subjects) == 2);
        assert (streq ((char *) zlist_first (subjects), "Name: Brutus"));
        assert (streq ((char *) zlist_next (subjects), "Age: 43"));
        zlist_destroy (&subjects);
        zlist_destroy (&consumer_hello_subjects);
        if (instance == MAX_INSTANCE - 1) {
            dafka_proto_destroy (&self);
            self = self_temp;
        }
    }
    dafka_proto_set_id (self, DAFKA_PROTO_STORE_HELLO);
    dafka_proto_set_topic (self, "Hello");
    dafka_proto_set_address (self, "Life is short but Now lasts for ever");
    // convert to zpl
    config = dafka_proto_zpl (self, NULL);
    if (verbose)
        zconfig_print (config);

    //  Send twice
    dafka_proto_send (self, output);
    dafka_proto_send (self, output);

    for (instance = 0; instance < MAX_INSTANCE; instance++) {
        dafka_proto_t *self_temp = self;
        if (instance < MAX_INSTANCE - 1)
            dafka_proto_recv (self, input);
        else {
            self = dafka_proto_new_zpl (config);
            assert (self);
            zconfig_destroy (&config);
        }
        if (instance < MAX_INSTANCE - 1)
            assert (dafka_proto_routing_id (self));
        assert (streq (dafka_proto_topic (self), "Hello"));
        assert (streq (dafka_proto_address (self), "Life is short but Now lasts for ever"));
        if (instance == MAX_INSTANCE - 1) {
            dafka_proto_destroy (&self);
            self = self_temp;
        }
    }

    dafka_proto_destroy (&self);
    zsock_destroy (&input);
    zsock_destroy (&output);
#if defined (__WINDOWS__)
    zsys_shutdown();
#endif
    //  @end

    printf ("OK\n");
}
