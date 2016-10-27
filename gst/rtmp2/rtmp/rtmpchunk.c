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
 * Free Software Foundation, Inc., 51 Franklin Street, Suite 500,
 * Boston, MA 02110-1335, USA.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gst/gst.h>
#include "rtmpchunk.h"
#include "rtmputils.h"
#include "rtmpdebug.h"
#include <string.h>

/* class initialization */

GstRtmpChunk *
gst_rtmp_chunk_new (void)
{
  return g_slice_new0 (GstRtmpChunk);
}

void
gst_rtmp_chunk_free (gpointer ptr)
{
  GstRtmpChunk *chunk = ptr;
  if (chunk->payload) {
    g_clear_pointer (&chunk->payload, g_bytes_unref);
  }
  g_slice_free (GstRtmpChunk, chunk);
}

guint32
gst_rtmp_chunk_parse_stream_id (const guint8 * data, gsize size)
{
  guint32 ret = 0;

  if (size >= 1) {
    ret = GST_READ_UINT8 (data) & 0x3f;

    if (ret == GST_RTMP_CHUNK_STREAM_TWOBYTE) {
      if (size >= 2) {
        ret = 64 + GST_READ_UINT8 (data + 1);
      } else {
        ret = 0;
      }
    } else if (ret == GST_RTMP_CHUNK_STREAM_THREEBYTE) {
      if (size >= 3) {
        ret = 64 + GST_READ_UINT16_LE (data + 1);
      } else {
        ret = 0;
      }
    }
  }

  return ret;
}

static const gsize header_sizes[4] = { 11, 7, 3, 0 };

gboolean
gst_rtmp_chunk_parse_header (GstRtmpChunkHeader * header, const guint8 * data,
    gsize size, GstRtmpChunkHeader * previous_header, gboolean continuation)
{
  const guint8 *msg_header_start, *payload_start;

  header->chunk_stream_id = gst_rtmp_chunk_parse_stream_id (data, size);

  g_warn_if_fail (previous_header->chunk_stream_id == header->chunk_stream_id);

  header->format = GST_READ_UINT8 (data) >> 6;
  header->header_size = header_sizes[header->format];

  switch (GST_READ_UINT8 (data) & 0x3f) {
    case GST_RTMP_CHUNK_STREAM_TWOBYTE:
      msg_header_start = data + 2;
      header->header_size += 2;
      break;
    case GST_RTMP_CHUNK_STREAM_THREEBYTE:
      msg_header_start = data + 3;
      header->header_size += 3;
      break;
    default:
      msg_header_start = data + 1;
      header->header_size += 1;
      break;
  }

  if (size < header->header_size) {
    GST_LOG ("not enough bytes to read header");
    return FALSE;
  }

  header->timestamp_abs = previous_header->timestamp_abs;
  header->timestamp_rel = previous_header->timestamp_rel;
  header->message_length = previous_header->message_length;
  header->message_type_id = previous_header->message_type_id;
  header->stream_id = previous_header->stream_id;

  payload_start = msg_header_start + header->header_size;

  switch (header->format) {
    case 0:
      header->timestamp_abs = 0;
      /* SRSLY:  "Message stream ID is stored in little-endian format." */
      header->stream_id = GST_READ_UINT32_LE (msg_header_start + 7);
      /* no break */
    case 1:
      header->message_type_id = GST_READ_UINT8 (msg_header_start + 6);
      header->message_length = GST_READ_UINT24_BE (msg_header_start + 3);
      /* no break */
    case 2:
      header->timestamp_rel = GST_READ_UINT24_BE (msg_header_start);

      if (header->timestamp_rel == 0xffffff) {
        GST_FIXME ("untested extended timestamp");

        header->header_size += 4;
        if (size < header->header_size) {
          GST_LOG ("not enough bytes to read extended timestamp");
          return FALSE;
        }

        header->timestamp_rel = GST_READ_UINT32_BE (payload_start);
        payload_start += 4;
      }
  }

  if (continuation && header->format != 3) {
    GST_ERROR ("expected message continuation, but got new message");
    continuation = FALSE;
  }

  if (!continuation) {
    header->timestamp_abs += header->timestamp_rel;
  }

  return TRUE;
}

GBytes *
gst_rtmp_chunk_serialize (GstRtmpChunk * chunk,
    GstRtmpChunkHeader * previous_header, gsize max_chunk_size)
{
  guint8 *data;
  const guint8 *chunkdata;
  gsize chunksize;
  int header_fmt;
  //guint32 timestamp;
  int offset;
  int i;

  chunkdata = g_bytes_get_data (chunk->payload, &chunksize);
  if (chunk->message_length != chunksize) {
    GST_ERROR ("message_length wrong (%" G_GSIZE_FORMAT " should be %"
        G_GSIZE_FORMAT ")", chunk->message_length, chunksize);
  }

  g_assert (chunk->chunk_stream_id < 64);
  data = g_malloc (chunksize + 12 + (chunksize / max_chunk_size));

  /* FIXME this is incomplete and inefficient */
  header_fmt = 0;
#if 0
  if (previous_header->message_length > 0) {
    header_fmt = 1;
    timestamp = chunk->timestamp - previous_header->timestamp;
  }
#endif

  g_assert (chunk->chunk_stream_id < 64);
  data[0] = (header_fmt << 6) | (chunk->chunk_stream_id);
  if (header_fmt == 0) {
    g_assert (chunk->timestamp < 0xffffff);
    GST_WRITE_UINT24_BE (data + 1, chunk->timestamp);
    GST_WRITE_UINT24_BE (data + 4, chunk->message_length);
    data[7] = chunk->message_type_id;
    /* SRSLY:  "Message stream ID is stored in little-endian format." */
    GST_WRITE_UINT32_LE (data + 8, chunk->stream_id);
    offset = 12;
  } else {
    GST_WRITE_UINT24_BE (data + 1, chunk->timestamp);
    GST_WRITE_UINT24_BE (data + 4, chunk->message_length);
    data[7] = chunk->message_type_id;
    offset = 8;
  }
  for (i = 0; i < (int) chunksize; i += max_chunk_size) {
    if (i != 0) {
      data[offset] = 0xc0 | chunk->chunk_stream_id;
      offset++;
    }
    memcpy (data + offset, chunkdata + i, MIN (chunksize - i, max_chunk_size));
    offset += MIN (chunksize - i, max_chunk_size);
  }
  GST_DEBUG ("type: %d in: %" G_GSIZE_FORMAT " out: %d", chunk->message_type_id,
      chunksize, offset);

  return g_bytes_new_take (data, offset);
}

/* chunk cache */

static void
gst_rtmp_chunk_cache_entry_clear (GstRtmpChunkCacheEntry * entry)
{
  g_clear_pointer (&entry->chunk, gst_rtmp_chunk_free);
}

GstRtmpChunkCache *
gst_rtmp_chunk_cache_new (void)
{
  GstRtmpChunkCache *cache =
      g_array_new (FALSE, TRUE, sizeof (GstRtmpChunkCacheEntry));
  g_array_set_clear_func (cache,
      (GDestroyNotify) gst_rtmp_chunk_cache_entry_clear);
  return cache;
}

void
gst_rtmp_chunk_cache_free (GstRtmpChunkCache * cache)
{
  g_array_free (cache, TRUE);

}

GstRtmpChunkCacheEntry *
gst_rtmp_chunk_cache_get (GstRtmpChunkCache * cache, guint32 chunk_stream_id)
{
  int i;
  GstRtmpChunkCacheEntry *entry;
  for (i = 0; i < (int) cache->len; i++) {
    entry = &g_array_index (cache, GstRtmpChunkCacheEntry, i);
    if (entry->previous_header.chunk_stream_id == chunk_stream_id)
      return entry;
  }
  g_array_set_size (cache, cache->len + 1);
  entry = &g_array_index (cache, GstRtmpChunkCacheEntry, cache->len - 1);
  entry->previous_header.chunk_stream_id = chunk_stream_id;
  return entry;
}

gboolean
gst_rtmp_chunk_parse_message (GstRtmpChunk * chunk, char **command_name,
    double *transaction_id, GstAmfNode ** command_object,
    GstAmfNode ** optional_args)
{
  gsize n_parsed;
  const guint8 *data;
  gsize size;
  int offset;
  GstAmfNode *n1, *n2, *n3, *n4;

  offset = 0;
  data = g_bytes_get_data (chunk->payload, &size);
  n1 = gst_amf_node_new_parse (data + offset, size - offset, &n_parsed);
  offset += n_parsed;
  n2 = gst_amf_node_new_parse (data + offset, size - offset, &n_parsed);
  offset += n_parsed;
  n3 = gst_amf_node_new_parse (data + offset, size - offset, &n_parsed);
  offset += n_parsed;
  if (offset < (int) size) {
    n4 = gst_amf_node_new_parse (data + offset, size - offset, &n_parsed);
  } else {
    n4 = NULL;
  }

  if (command_name) {
    *command_name = g_strdup (gst_amf_node_get_string (n1));
  }
  gst_amf_node_free (n1);

  if (transaction_id) {
    *transaction_id = gst_amf_node_get_number (n2);
  }
  gst_amf_node_free (n2);

  if (command_object) {
    *command_object = n3;
  } else {
    gst_amf_node_free (n3);
  }

  if (optional_args) {
    *optional_args = n4;
  } else {
    if (n4)
      gst_amf_node_free (n4);
  }

  return TRUE;
}
