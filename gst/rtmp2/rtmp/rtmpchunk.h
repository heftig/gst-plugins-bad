/* GStreamer RTMP Library
 * Copyright (C) 2013 David Schleef <ds@schleef.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#ifndef _GST_RTMP_CHUNK_H_
#define _GST_RTMP_CHUNK_H_

#include <glib.h>
#include "rtmp/amf.h"

G_BEGIN_DECLS

#define GST_TYPE_RTMP_CHUNK   (gst_rtmp_chunk_get_type())
#define GST_RTMP_CHUNK(obj)   (G_TYPE_CHECK_INSTANCE_CAST((obj),GST_TYPE_RTMP_CHUNK,GstRtmpChunk))
#define GST_RTMP_CHUNK_CLASS(klass)   (G_TYPE_CHECK_CLASS_CAST((klass),GST_TYPE_RTMP_CHUNK,GstRtmpChunkClass))
#define GST_IS_RTMP_CHUNK(obj)   (G_TYPE_CHECK_INSTANCE_TYPE((obj),GST_TYPE_RTMP_CHUNK))
#define GST_IS_RTMP_CHUNK_CLASS(obj)   (G_TYPE_CHECK_CLASS_TYPE((klass),GST_TYPE_RTMP_CHUNK))

typedef struct _GstRtmpChunk GstRtmpChunk;
typedef struct _GstRtmpChunkClass GstRtmpChunkClass;
typedef GArray GstRtmpChunkCache;
typedef struct _GstRtmpChunkCacheEntry GstRtmpChunkCacheEntry;
typedef struct _GstRtmpChunkHeader GstRtmpChunkHeader;

struct _GstRtmpChunkHeader {
  int format;
  gsize header_size;
  guint32 chunk_stream_id;
  guint32 timestamp;
  gsize message_length;
  int message_type_id;
  guint32 stream_id;
};

struct _GstRtmpChunkCacheEntry {
  GstRtmpChunkHeader previous_header;
  GstRtmpChunk *chunk;
  guint8 *payload;
  gsize offset;
};

struct _GstRtmpChunk
{
  GObject object;

  guint32 chunk_stream_id;
  guint32 timestamp;
  gsize message_length;
  int message_type_id;
  guint32 stream_id;

  GBytes *payload;
};

struct _GstRtmpChunkClass
{
  GObjectClass object_class;
};

typedef enum {
  GST_RTMP_CHUNK_PARSE_ERROR = 0,
  GST_RTMP_CHUNK_PARSE_OK,
  GST_RTMP_CHUNK_PARSE_UNKNOWN,
  GST_RTMP_CHUNK_PARSE_NEED_BYTES,
} GstRtmpChunkParseStatus;

typedef enum {
  GST_RTMP_CHUNK_STREAM_TWOBYTE = 0,
  GST_RTMP_CHUNK_STREAM_THREEBYTE = 1,
  GST_RTMP_CHUNK_STREAM_PROTOCOL = 2,
} GstRtmpChunkStream;

typedef enum {
  GST_RTMP_MESSAGE_TYPE_SET_CHUNK_SIZE = 1,
  GST_RTMP_MESSAGE_TYPE_ABORT = 2,
  GST_RTMP_MESSAGE_TYPE_ACKNOWLEDGEMENT = 3,
  GST_RTMP_MESSAGE_TYPE_USER_CONTROL = 4,
  GST_RTMP_MESSAGE_TYPE_WINDOW_ACK_SIZE = 5,
  GST_RTMP_MESSAGE_TYPE_SET_PEER_BANDWIDTH = 6,
  GST_RTMP_MESSAGE_TYPE_AUDIO = 8,
  GST_RTMP_MESSAGE_TYPE_VIDEO = 9,
  GST_RTMP_MESSAGE_TYPE_DATA_AMF3 = 15,
  GST_RTMP_MESSAGE_TYPE_SHARED_OBJECT_AMF3 = 16,
  GST_RTMP_MESSAGE_TYPE_COMMAND_AMF3 = 17,
  GST_RTMP_MESSAGE_TYPE_DATA = 18,
  GST_RTMP_MESSAGE_TYPE_SHARED_OBJECT = 19,
  GST_RTMP_MESSAGE_TYPE_COMMAND = 20,
  GST_RTMP_MESSAGE_TYPE_AGGREGATE = 22,
} GstRtmpMessageType;

typedef enum {
  GST_RTMP_USER_CONTROL_STREAM_BEGIN = 0,
  GST_RTMP_USER_CONTROL_STREAM_EOF = 1,
  GST_RTMP_USER_CONTROL_STREAM_DRY = 2,
  GST_RTMP_USER_CONTROL_SET_BUFFER_LENGTH = 3,
  GST_RTMP_USER_CONTROL_STREAM_IS_RECORDED = 4,
  GST_RTMP_USER_CONTROL_PING_REQUEST = 6,
  GST_RTMP_USER_CONTROL_PING_RESPONSE = 7,
} GstRtmpUserControl;

GType gst_rtmp_chunk_get_type (void);

GstRtmpChunk *gst_rtmp_chunk_new (void);
GBytes * gst_rtmp_chunk_serialize (GstRtmpChunk *chunk,
    GstRtmpChunkHeader *previous_header, gsize max_chunk_size);

void gst_rtmp_chunk_set_chunk_stream_id (GstRtmpChunk *chunk, guint32 chunk_stream_id);
void gst_rtmp_chunk_set_timestamp (GstRtmpChunk *chunk, guint32 timestamp);
void gst_rtmp_chunk_set_payload (GstRtmpChunk *chunk, GBytes *payload);

guint32 gst_rtmp_chunk_get_chunk_stream_id (GstRtmpChunk *chunk);
guint32 gst_rtmp_chunk_get_timestamp (GstRtmpChunk *chunk);
GBytes * gst_rtmp_chunk_get_payload (GstRtmpChunk *chunk);

gboolean gst_rtmp_chunk_parse_header1 (GstRtmpChunkHeader *header, GBytes * bytes);
gboolean gst_rtmp_chunk_parse_header2 (GstRtmpChunkHeader *header, GBytes * bytes,
    GstRtmpChunkHeader *previous_header);
gboolean gst_rtmp_chunk_parse_message (GstRtmpChunk *chunk,
    char **command_name, double *transaction_id,
    GstAmfNode **command_object, GstAmfNode **optional_args);



/* chunk cache */

GstRtmpChunkCache *gst_rtmp_chunk_cache_new (void);
void gst_rtmp_chunk_cache_free (GstRtmpChunkCache *cache);
GstRtmpChunkCacheEntry * gst_rtmp_chunk_cache_get (
    GstRtmpChunkCache *cache, guint32 chunk_stream_id);
void gst_rtmp_chunk_cache_update (GstRtmpChunkCacheEntry * entry,
    GstRtmpChunk * chunk);

G_END_DECLS

#endif
