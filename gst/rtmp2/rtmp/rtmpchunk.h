/* GStreamer RTMP Library
 * Copyright (C) 2013 David Schleef <ds@schleef.org>
 * Copyright (C) 2017 Make.TV, Inc. <info@make.tv>
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

typedef struct _GstRtmpChunk GstRtmpChunk;
typedef GArray GstRtmpChunkCache;
typedef struct _GstRtmpChunkCacheEntry GstRtmpChunkCacheEntry;
typedef struct _GstRtmpChunkHeader GstRtmpChunkHeader;

struct _GstRtmpChunkHeader {
  gint format;
  gsize header_size;
  guint32 chunk_stream_id;
  guint32 timestamp_abs, timestamp_rel;
  gsize message_length;
  gint message_type_id;
  guint32 stream_id;
};

struct _GstRtmpChunkCacheEntry {
  GstRtmpChunkHeader previous_header;
  GstRtmpChunk *chunk;
  gsize offset;
};

struct _GstRtmpChunk
{
  guint32 chunk_stream_id;
  guint32 timestamp;
  gsize message_length;
  gint message_type_id;
  guint32 stream_id;

  GBytes *payload;
};

typedef enum {
  GST_RTMP_CHUNK_STREAM_TWOBYTE = 0,
  GST_RTMP_CHUNK_STREAM_THREEBYTE = 1,
  GST_RTMP_CHUNK_STREAM_PROTOCOL = 2,
} GstRtmpChunkStream;

GstRtmpChunk *gst_rtmp_chunk_new (void);
void gst_rtmp_chunk_free (gpointer ptr);
GBytes * gst_rtmp_chunk_serialize (GstRtmpChunk *chunk,
    GstRtmpChunkHeader *previous_header, gsize max_chunk_size);

guint32 gst_rtmp_chunk_parse_stream_id (const guint8 * data, gsize size);
gboolean gst_rtmp_chunk_parse_header (GstRtmpChunkHeader *header,
    const guint8 * data, gsize size, GstRtmpChunkHeader *previous_header,
    gboolean continuation);

/* chunk cache */

GstRtmpChunkCache *gst_rtmp_chunk_cache_new (void);
void gst_rtmp_chunk_cache_free (GstRtmpChunkCache *cache);
GstRtmpChunkCacheEntry * gst_rtmp_chunk_cache_get (
    GstRtmpChunkCache *cache, guint32 chunk_stream_id);

G_END_DECLS

#endif
