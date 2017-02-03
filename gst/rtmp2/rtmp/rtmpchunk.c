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

  payload_start = data + header->header_size;

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

static inline guint8
select_message_header_fmt (GstRtmpChunk * chunk,
    GstRtmpChunkHeader * previous_header, guint32 * timestamp,
    gboolean * ext_ts)
{
  *timestamp = chunk->timestamp;
  *ext_ts = (*timestamp > 0xfffffe);

  if (previous_header == NULL) {
    GST_DEBUG ("picking chunk header 0: no previous header");
    return 0;
  }

  g_return_val_if_fail (previous_header->chunk_stream_id ==
      chunk->chunk_stream_id, 0);

  if (previous_header->stream_id != chunk->stream_id) {
    GST_DEBUG ("picking chunk header 0: stream-id mismatch; "
        "want %" G_GUINT32_FORMAT " got %" G_GUINT32_FORMAT,
        previous_header->stream_id, chunk->stream_id);
    return 0;
  }

  *timestamp -= previous_header->timestamp_abs;
  *ext_ts = (*timestamp > 0xfffffe);

  if (previous_header->message_type_id != chunk->message_type_id) {
    GST_LOG ("picking chunk header 1: message type mismatch; want %d got %d",
        previous_header->message_type_id, chunk->message_type_id);
    return 1;
  }

  if (previous_header->message_length != chunk->message_length) {
    GST_LOG ("picking chunk header 1: message length mismatch; "
        "want %" G_GSIZE_FORMAT " got %" G_GSIZE_FORMAT,
        previous_header->message_length, chunk->message_length);
    return 1;
  }

  if (previous_header->timestamp_rel != *timestamp) {
    GST_LOG ("picking chunk header 2: timestamp delta mismatch; "
        "want %" G_GUINT32_FORMAT " got %" G_GUINT32_FORMAT,
        previous_header->timestamp_rel, *timestamp);
    return 2;
  }

  GST_LOG ("picking chunk header 3");
  *ext_ts = FALSE;
  return 3;
}

GBytes *
gst_rtmp_chunk_serialize (GstRtmpChunk * chunk,
    GstRtmpChunkHeader * previous_header, gsize max_chunk_size)
{
  guint8 *data, format;
  const guint8 *payload_data;
  gsize payload_size, offset, i, basic_header_size, num_chunks;
  guint32 small_stream_id, timestamp;
  gboolean ext_ts;

  payload_data = g_bytes_get_data (chunk->payload, &payload_size);
  g_return_val_if_fail (chunk->message_length == payload_size, NULL);

  small_stream_id = chunk->chunk_stream_id;
  if (small_stream_id < 2) {
    g_return_val_if_reached (NULL);
  } else if (small_stream_id < 0x40) {
    basic_header_size = 1;
  } else if (small_stream_id < 0x140) {
    basic_header_size = 2;
    small_stream_id = GST_RTMP_CHUNK_STREAM_TWOBYTE;
  } else if (small_stream_id < 0x10040) {
    basic_header_size = 3;
    small_stream_id = GST_RTMP_CHUNK_STREAM_THREEBYTE;
  } else {
    g_return_val_if_reached (NULL);
  }

  if (previous_header &&
      previous_header->chunk_stream_id != chunk->chunk_stream_id) {
    g_warn_if_reached ();
    previous_header = NULL;
  }

  format = select_message_header_fmt (chunk, previous_header, &timestamp,
      &ext_ts);
  num_chunks = (payload_size + max_chunk_size - 1) / max_chunk_size;
  data = g_malloc (header_sizes[format] + (ext_ts ? 4 : 0) + payload_size +
      num_chunks * basic_header_size);
  offset = 0;

  for (i = 0; i < num_chunks; i++) {
    gsize chunk_size = MIN (payload_size, max_chunk_size);

    GST_WRITE_UINT8 (data + offset, (format << 6) | small_stream_id);

    switch (small_stream_id) {
      case GST_RTMP_CHUNK_STREAM_TWOBYTE:
        GST_WRITE_UINT8 (data + offset + 1, chunk->chunk_stream_id - 0x40);
        break;
      case GST_RTMP_CHUNK_STREAM_THREEBYTE:
        GST_WRITE_UINT16_LE (data + offset + 1, chunk->chunk_stream_id - 0x40);
        break;
    }
    offset += basic_header_size;

    switch (format) {
      case 0:
        /* SRSLY:  "Message stream ID is stored in little-endian format." */
        GST_WRITE_UINT32_LE (data + offset + 7, chunk->stream_id);
        /* no break */
      case 1:
        GST_WRITE_UINT24_BE (data + offset + 3, chunk->message_length);
        GST_WRITE_UINT8 (data + offset + 6, chunk->message_type_id);
        /* no break */
      case 2:
        GST_WRITE_UINT24_BE (data + offset, ext_ts ? 0xffffff : timestamp);

        offset += header_sizes[format];

        if (ext_ts) {
          GST_WRITE_UINT32_BE (data + offset, timestamp);
          offset += 4;
        }
    }

    memcpy (data + offset, payload_data, chunk_size);
    payload_data += chunk_size;
    payload_size -= chunk_size;
    offset += chunk_size;

    format = 3;
    ext_ts = FALSE;
  }

  if (previous_header) {
    previous_header->format = format;
    previous_header->timestamp_abs = chunk->timestamp;
    previous_header->timestamp_rel = timestamp;
    previous_header->message_length = chunk->message_length;
    previous_header->message_type_id = chunk->message_type_id;
    previous_header->stream_id = chunk->stream_id;
  }

  GST_LOG ("serialized chunk type %d %" G_GSIZE_FORMAT " -> %" G_GSIZE_FORMAT
      " bytes", chunk->message_type_id, g_bytes_get_size (chunk->payload),
      offset);

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
  guint i;
  GstRtmpChunkCacheEntry *entry;

  for (i = 0; i < cache->len; i++) {
    entry = &g_array_index (cache, GstRtmpChunkCacheEntry, i);
    if (entry->previous_header.chunk_stream_id == chunk_stream_id)
      return entry;
  }

  g_array_set_size (cache, i + 1);
  entry = &g_array_index (cache, GstRtmpChunkCacheEntry, i);
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
